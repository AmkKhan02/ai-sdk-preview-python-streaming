"use client";

import type { ChatRequestOptions, CreateMessage, Message } from "ai";
import { motion } from "framer-motion";
import type React from "react";
import {
  useRef,
  useEffect,
  useCallback,
  useState,
  type Dispatch,
  type SetStateAction,
} from "react";
import { toast } from "sonner";
import { useLocalStorage, useWindowSize } from "usehooks-ts";
import GenerateSchema from "generate-schema";
import Papa from "papaparse";
import { processDuckDBFile, terminateDuckDB } from "@/lib/utils";

import { cn, sanitizeUIMessages } from "@/lib/utils";

import { ArrowUpIcon, StopIcon } from "./icons";
import { Paperclip } from "./ui/paperclip";
import { Button } from "./ui/button";
import { Textarea } from "./ui/textarea";
import { PreviewAttachment } from "./preview-attachment";

const suggestedActions = [
  {
    title: "What is the weather",
    label: "in San Francisco?",
    action: "What is the weather in San Francisco?",
  },
  {
    title: "How is python useful",
    label: "for AI engineers?",
    action: "How is python useful for AI engineers?",
  },
];

export function MultimodalInput({
  chatId,
  input,
  setInput,
  isLoading,
  stop,
  messages,
  setMessages,
  append,
  handleSubmit,
  className,
}: {
  chatId: string;
  input: string;
  setInput: (value: string) => void;
  isLoading: boolean;
  stop: () => void;
  messages: Array<Message>;
  setMessages: Dispatch<SetStateAction<Array<Message>>>;
  append: (
    message: Message | CreateMessage,
    chatRequestOptions?: ChatRequestOptions,
  ) => Promise<string | null | undefined>;
  handleSubmit: (
    event?: {
      preventDefault?: () => void;
    },
    chatRequestOptions?: ChatRequestOptions,
  ) => void;
  className?: string;
}) {
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const { width } = useWindowSize();
  const [attachment, setAttachment] = useState<File | undefined>();
  const [isUploading, setIsUploading] = useState(false);
  const [jsonSchema, setJsonSchema] = useState<any | null>(null);
  const [parsedData, setParsedData] = useState<any[] | null>(null);
  const [isGeneratingSchema, setIsGeneratingSchema] = useState(false);

  // Cleanup DuckDB on unmount
  useEffect(() => {
    return () => {
      terminateDuckDB();
    };
  }, []);

  useEffect(() => {
    if (textareaRef.current) {
      adjustHeight();
    }
  }, []);

  const adjustHeight = () => {
    if (textareaRef.current) {
      textareaRef.current.style.height = "auto";
      textareaRef.current.style.height = `${textareaRef.current.scrollHeight + 2}px`;
    }
  };

  const [localStorageInput, setLocalStorageInput] = useLocalStorage(
    "input",
    "",
  );

  useEffect(() => {
    if (textareaRef.current) {
      const domValue = textareaRef.current.value;
      // Prefer DOM value over localStorage to handle hydration
      const finalValue = domValue || localStorageInput || "";
      setInput(finalValue);
      adjustHeight();
    }
    // Only run once after hydration
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    setLocalStorageInput(input);
  }, [input, setLocalStorageInput]);

  const handleInput = (event: React.ChangeEvent<HTMLTextAreaElement>) => {
    setInput(event.target.value);
    adjustHeight();
  };

  const submitForm = useCallback(() => {
    let messageContent = input;

    // If we have parsed data from CSV, JSON, or DuckDB, include it in the message content
    if (parsedData && attachment) {
      const dataString = JSON.stringify(parsedData, null, 2);
      const fileType = attachment.name.endsWith('.duckdb') || attachment.name.endsWith('.db') ? 'DuckDB database' : 
                      attachment.name.endsWith('.csv') ? 'CSV file' : 'JSON file';
      messageContent = `${input}\n\nI've uploaded a ${fileType} (${attachment.name}) with the following data:\n\`\`\`json\n${dataString}\n\`\`\`\n\nPlease create a bar graph from this data.`;
    }

    // For image attachments, we still need to handle them differently
    if (attachment && attachment.type.startsWith('image/')) {
      setIsUploading(true);
      const reader = new FileReader();
      reader.onloadend = () => {
        const message = {
          role: "user" as const,
          content: messageContent,
          experimental_attachments: [
            {
              name: attachment.name,
              contentType: attachment.type,
              url: reader.result as string,
            },
          ],
        };
        console.log("Sending message with image:", message);
        append(message, {});
        setAttachment(undefined);
        setParsedData(null);
        setJsonSchema(null);
        setIsUploading(false);
      };
      reader.onerror = () => {
        toast.error("Failed to read file.");
        setIsUploading(false);
      };
      reader.readAsDataURL(attachment);
    } else {
      // For CSV files or no attachment, send as text message only
      const message = {
        role: "user" as const,
        content: messageContent,
      };
      console.log("Sending message:", message);
      append(message, {});
      setAttachment(undefined);
      setParsedData(null);
      setJsonSchema(null);
    }

    setInput("");
    setLocalStorageInput("");

    if (width && width > 768) {
      textareaRef.current?.focus();
    }
  }, [
    handleSubmit,
    setLocalStorageInput,
    width,
    attachment,
    append,
    input,
    setInput,
    parsedData,
  ]);

  const handleFileChange = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    setAttachment(file);
    setIsGeneratingSchema(true);
    setJsonSchema(null);
    setParsedData(null);

    try {
      if (file.type === "application/json") {
        // Handle JSON files
        const reader = new FileReader();
        reader.onload = (e) => {
          try {
            const text = e.target?.result as string;
            const data = JSON.parse(text);
            const schema = GenerateSchema.json(data);
            setParsedData(Array.isArray(data) ? data : [data]);
            setJsonSchema(schema);
          } catch (error) {
            toast.error("Invalid JSON file");
            console.error("JSON parsing error:", error);
          }
          setIsGeneratingSchema(false);
        };
        reader.onerror = () => {
          toast.error("Failed to read JSON file");
          setIsGeneratingSchema(false);
        };
        reader.readAsText(file);
      } 
      else if (file.type === "text/csv" || file.name.endsWith('.csv')) {
        // Handle CSV files
        const reader = new FileReader();
        reader.onload = (e) => {
          try {
            const text = e.target?.result as string;
            const parsedCsv = Papa.parse(text, { 
              header: true, 
              skipEmptyLines: true,
              dynamicTyping: true
            });
            
            if (parsedCsv.errors.length > 0) {
              console.warn("CSV parsing warnings:", parsedCsv.errors);
            }
            
            const data = parsedCsv.data;
            const schema = GenerateSchema.json(data);
            setParsedData(data);
            setJsonSchema(schema);
            
            console.log("Parsed CSV data:", data);
          } catch (error) {
            toast.error("Failed to parse CSV file");
            console.error("CSV parsing error:", error);
          }
          setIsGeneratingSchema(false);
        };
        reader.onerror = () => {
          toast.error("Failed to read CSV file");
          setIsGeneratingSchema(false);
        };
        reader.readAsText(file);
      }
      else if (file.name.endsWith('.duckdb') || file.name.endsWith('.db')) {
        // Handle DuckDB files
        await handleDuckDBFile(file);
      }
      else {
        toast.error("Unsupported file type. Please upload CSV, JSON, or DuckDB files.");
        setAttachment(undefined);
        setIsGeneratingSchema(false);
      }
    } catch (error) {
      console.error("Error processing file:", error);
      toast.error("Failed to process file.");
      setIsGeneratingSchema(false);
    }
  };

  const handleDuckDBFile = async (file: File) => {
    try {
      const result = await processDuckDBFile(file);
      
      // Generate schema and set data
      const schema = GenerateSchema.json(result.data);
      setParsedData(result.data);
      setJsonSchema(schema);
      
      toast.success(`Successfully loaded ${result.rowCount} rows from table "${result.tableName}"`);
      
    } catch (error) {
      console.error("DuckDB parsing error:", error);
      toast.error(error instanceof Error ? error.message : "Failed to parse DuckDB file. Make sure it's a valid DuckDB database.");
    } finally {
      setIsGeneratingSchema(false);
    }
  };

  return (
    <div className="relative w-full flex flex-col gap-4">
      {messages.length === 0 && (
        <div className="grid sm:grid-cols-2 gap-2 w-full">
          {suggestedActions.map((suggestedAction, index) => (
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: 20 }}
              transition={{ delay: 0.05 * index }}
              key={`suggested-action-${suggestedAction.title}-${index}`}
              className={index > 1 ? "hidden sm:block" : "block"}
            >
              <Button
                variant="ghost"
                onClick={async () => {
                  append({
                    role: "user",
                    content: suggestedAction.action,
                  });
                }}
                className="text-left border rounded-xl px-4 py-3.5 text-sm flex-1 gap-1 sm:flex-col w-full h-auto justify-start items-start"
              >
                <span className="font-medium">{suggestedAction.title}</span>
                <span className="text-muted-foreground">
                  {suggestedAction.label}
                </span>
              </Button>
            </motion.div>
          ))}
        </div>
      )}

      {attachment && (
        <PreviewAttachment
          attachment={{
            name: attachment.name,
            contentType: attachment.type,
            url: URL.createObjectURL(attachment),
          }}
          setAttachment={(file) => {
            setAttachment(file as File | undefined);
            if (!file) {
              setParsedData(null);
              setJsonSchema(null);
            }
          }}
          isUploading={isUploading}
          isGeneratingSchema={isGeneratingSchema}
          jsonSchema={jsonSchema}
        />
      )}

      <div className="relative flex items-center">
        <Button
          className="rounded-full p-1.5 h-fit absolute left-2 m-0.5 border dark:border-zinc-600"
          onClick={(event) => {
            event.preventDefault();
            fileInputRef.current?.click();
          }}
          disabled={isUploading}
        >
          <Paperclip className="w-4 h-4" />
        </Button>

        <input
          ref={fileInputRef}
          type="file"
          accept=".csv,.json,.duckdb,.db"
          className="hidden"
          onChange={handleFileChange}
        />

        <Textarea
          ref={textareaRef}
          style={{ paddingLeft: "40px" }}
          placeholder="Send a message..."
          value={input}
          onChange={handleInput}
          className={cn(
            "min-h-[24px] max-h-[calc(75dvh)] overflow-hidden resize-none rounded-xl !text-base bg-muted",
            className,
          )}
          rows={3}
          autoFocus
          onKeyDown={(event) => {
            if (event.key === "Enter" && !event.shiftKey) {
              event.preventDefault();

              if (isLoading || isUploading) {
                toast.error("Please wait for the model to finish its response!");
              } else {
                submitForm();
              }
            }
          }}
        />
      </div>

      {isLoading || isUploading ? (
        <Button
          className="rounded-full p-1.5 h-fit absolute bottom-2 right-2 m-0.5 border dark:border-zinc-600"
          onClick={(event) => {
            event.preventDefault();
            if (isLoading) {
              stop();
              setMessages((messages) => sanitizeUIMessages(messages));
            }
          }}
          disabled={!isLoading}
        >
          <StopIcon size={14} />
        </Button>
      ) : (
        <Button
          className="rounded-full p-1.5 h-fit absolute bottom-2 right-2 m-0.5 border dark:border-zinc-600"
          onClick={(event) => {
            event.preventDefault();
            submitForm();
          }}
          disabled={input.length === 0 && !attachment}
        >
          <ArrowUpIcon size={14} />
        </Button>
      )}
    </div>
  );
}