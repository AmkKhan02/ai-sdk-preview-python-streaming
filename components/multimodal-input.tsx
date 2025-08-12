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
import { terminateDuckDB } from "@/lib/utils";

import { cn, sanitizeUIMessages } from "@/lib/utils";

import { ArrowUpIcon, StopIcon } from "./icons";
import { AttachmentButton } from "./ui/attachment-button";
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
  const { width } = useWindowSize();
  
  // Separate state management for CSV and DuckDB files
  const [csvAttachment, setCsvAttachment] = useState<File | undefined>();
  const [duckdbAttachment, setDuckdbAttachment] = useState<File | undefined>();
  const [isUploading, setIsUploading] = useState(false);
  const [jsonSchema, setJsonSchema] = useState<any | null>(null);
  const [parsedData, setParsedData] = useState<any[] | null>(null);
  const [isGeneratingSchema, setIsGeneratingSchema] = useState(false);
  const [duckdbColumns, setDuckdbColumns] = useState<string[] | null>(null);
  const [isDuckdbUploading, setIsDuckdbUploading] = useState(false);
  const [webSearchEnabled, setWebSearchEnabled] = useState(false);
  
  // Get the current attachment (either CSV or DuckDB)
  const attachment = csvAttachment || duckdbAttachment;

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

    // If we have parsed data from CSV or DuckDB, include it in the message content
    if (parsedData && attachment) {
      const dataString = JSON.stringify(parsedData, null, 2);
      const fileType = duckdbAttachment ? 'DuckDB database' : 'CSV file';
      messageContent = `${input}\n\nI've uploaded a ${fileType} (${attachment.name}) with the following data:\n\`\`\`json\n${dataString}\n\`\`\`\n`;
    }

    // For DuckDB files with column information only
    if (duckdbColumns && duckdbAttachment && !parsedData) {
      messageContent = `${input}\n\nI've uploaded a DuckDB database (${duckdbAttachment.name}) with the following columns:\n${duckdbColumns.join(', ')}`;
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
        append(message, {
          body: {
            webSearchEnabled
          }
        });
        clearAttachments();
        setIsUploading(false);
      };
      reader.onerror = () => {
        toast.error("Failed to read file.");
        setIsUploading(false);
      };
      reader.readAsDataURL(attachment);
    } else {
      // For CSV/DuckDB files or no attachment, send as text message only
      const message = {
        role: "user" as const,
        content: messageContent,
      };
      console.log("Sending message:", message);
      append(message, {
        body: {
          webSearchEnabled
        }
      });
      clearAttachments();
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
    csvAttachment,
    duckdbAttachment,
    append,
    input,
    setInput,
    parsedData,
    duckdbColumns,
  ]);

  const clearAttachments = () => {
    setCsvAttachment(undefined);
    setDuckdbAttachment(undefined);
    setParsedData(null);
    setJsonSchema(null);
    setDuckdbColumns(null);
  };

  const handleCsvFileSelect = async (file: File) => {
    // File type validation for CSV button
    if (!file.name.endsWith('.csv') && file.type !== 'text/csv') {
      toast.error("Please select a CSV file (.csv extension required)");
      return;
    }

    // Clear any existing attachments
    clearAttachments();
    setCsvAttachment(file);
    setIsGeneratingSchema(true);

    try {
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
          toast.success(`Successfully loaded ${data.length} rows from CSV file`);
        } catch (error) {
          toast.error("Failed to parse CSV file");
          console.error("CSV parsing error:", error);
          setCsvAttachment(undefined);
        }
        setIsGeneratingSchema(false);
      };
      reader.onerror = () => {
        toast.error("Failed to read CSV file");
        setIsGeneratingSchema(false);
        setCsvAttachment(undefined);
      };
      reader.readAsText(file);
    } catch (error) {
      console.error("Error processing CSV file:", error);
      toast.error("Failed to process CSV file.");
      setIsGeneratingSchema(false);
      setCsvAttachment(undefined);
    }
  };

  const handleDuckdbFileSelect = async (file: File) => {
    // File type validation for DuckDB button
    if (!file.name.endsWith('.duckdb') && !file.name.endsWith('.db')) {
      toast.error("Please select a DuckDB file (.duckdb or .db extension required)");
      return;
    }

    // Clear any existing attachments
    clearAttachments();
    setDuckdbAttachment(file);
    setIsDuckdbUploading(true);

    try {
      // Upload file to backend for processing
      const formData = new FormData();
      formData.append('file', file);

      const response = await fetch('/api/upload-duckdb', {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ detail: 'Unknown error' }));
        throw new Error(errorData.detail || `HTTP ${response.status}: ${response.statusText}`);
      }

      const result = await response.json();
      
      if (result.status === 'error') {
        throw new Error(result.error || 'Failed to process DuckDB file');
      }

      // Set the column information
      setDuckdbColumns(result.columns);
      
      toast.success(`Successfully processed DuckDB file: ${result.columns.length} columns found in table "${result.table_name}"`);
      
    } catch (error) {
      console.error("Error processing DuckDB file:", error);
      const errorMessage = error instanceof Error ? error.message : "Failed to process DuckDB file.";
      toast.error(errorMessage);
      setDuckdbAttachment(undefined);
    } finally {
      setIsDuckdbUploading(false);
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
            if (!file) {
              clearAttachments();
            }
          }}
          isUploading={isUploading || isDuckdbUploading}
          isGeneratingSchema={isGeneratingSchema}
          jsonSchema={jsonSchema}
          duckdbColumns={duckdbColumns}
        />
      )}

      <div className="relative flex items-center">
        <div className="absolute left-2 m-0.5 flex gap-1">
          <AttachmentButton
            fileType="csv"
            onFileSelect={handleCsvFileSelect}
            disabled={isUploading || isDuckdbUploading || isGeneratingSchema}
            accept=".csv"
            tooltip="Upload CSV file"
          />
          <AttachmentButton
            fileType="duckdb"
            onFileSelect={handleDuckdbFileSelect}
            disabled={isUploading || isDuckdbUploading || isGeneratingSchema}
            accept=".duckdb,.db"
            tooltip="Upload DuckDB database file"
          />
          <Button
            type="button"
            variant="outline"
            size="icon"
            onClick={(event) => {
              event.preventDefault();
              setWebSearchEnabled(!webSearchEnabled);
            }}
            className={cn(
              "rounded-full p-1.5 h-fit border dark:border-zinc-600 transition-colors duration-200",
              webSearchEnabled 
                ? "bg-orange-50 border-orange-200 text-orange-700 hover:bg-orange-100 hover:border-orange-300 dark:bg-orange-950 dark:border-orange-800 dark:text-orange-300 dark:hover:bg-orange-900 dark:hover:border-orange-700" 
                : "hover:bg-orange-50 hover:border-orange-200 hover:text-orange-700 dark:hover:bg-orange-950 dark:hover:border-orange-800 dark:hover:text-orange-300"
            )}
            title={webSearchEnabled ? "Disable web search" : "Enable web search"}
          >
            <svg 
              width="16" 
              height="16" 
              viewBox="0 0 24 24" 
              fill="none" 
              xmlns="http://www.w3.org/2000/svg"
              className="w-4 h-4"
            >
              <circle cx="11" cy="11" r="8" stroke="currentColor" strokeWidth="2"/>
              <path d="M21 21L16.5 16.5" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
              <path d="M11 6C13.5 8.5 13.5 13.5 11 16" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
              <path d="M6 11H16" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
            </svg>
          </Button>
        </div>

        <Textarea
          ref={textareaRef}
          style={{ paddingLeft: "144px" }}
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

              if (isLoading || isUploading || isDuckdbUploading) {
                toast.error("Please wait for the model to finish its response!");
              } else {
                submitForm();
              }
            }
          }}
        />
      </div>

      {isLoading || isUploading || isDuckdbUploading ? (
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
