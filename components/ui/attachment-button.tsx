"use client";

import React from "react";
import { Button } from "./button";
import { cn } from "@/lib/utils";

// CSV Icon component
const CsvIcon = ({ size = 16 }: { size?: number }) => (
  <svg
    height={size}
    strokeLinejoin="round"
    viewBox="0 0 16 16"
    width={size}
    style={{ color: "currentcolor" }}
  >
    <path
      fillRule="evenodd"
      clipRule="evenodd"
      d="M14.5 13.5V6.5V5.41421C14.5 5.149 14.3946 4.89464 14.2071 4.70711L9.79289 0.292893C9.60536 0.105357 9.351 0 9.08579 0H8H3H1.5V1.5V13.5C1.5 14.8807 2.61929 16 4 16H12C13.3807 16 14.5 14.8807 14.5 13.5ZM13 13.5V6.5H9.5H8V5V1.5H3V13.5C3 14.0523 3.44772 14.5 4 14.5H12C12.5523 14.5 13 14.0523 13 13.5ZM9.5 5V2.12132L12.3787 5H9.5ZM4.5 8.5H5.5C6.05228 8.5 6.5 8.94772 6.5 9.5V10.5C6.5 11.0523 6.05228 11.5 5.5 11.5H4.5C3.94772 11.5 3.5 11.0523 3.5 10.5V9.5C3.5 8.94772 3.94772 8.5 4.5 8.5ZM8.5 8.5H9.5C10.0523 8.5 10.5 8.94772 10.5 9.5V10.5C10.5 11.0523 10.0523 11.5 9.5 11.5H8.5C7.94772 11.5 7.5 11.0523 7.5 10.5V9.5C7.5 8.94772 7.94772 8.5 8.5 8.5ZM11.5 8.5H12.5C13.0523 8.5 13.5 8.94772 13.5 9.5V10.5C13.5 11.0523 13.0523 11.5 12.5 11.5H11.5C10.9477 11.5 10.5 11.0523 10.5 10.5V9.5C10.5 8.94772 10.9477 8.5 11.5 8.5Z"
      fill="currentColor"
    />
  </svg>
);

// DuckDB Icon component
const DuckDbIcon = ({ size = 16 }: { size?: number }) => (
  <svg
    height={size}
    strokeLinejoin="round"
    viewBox="0 0 16 16"
    width={size}
    style={{ color: "currentcolor" }}
  >
    <path
      fillRule="evenodd"
      clipRule="evenodd"
      d="M8 0.154663L8.34601 0.334591L14.596 3.58459L15 3.79466V4.25V11.75V12.2053L14.596 12.4154L8.34601 15.6654L8 15.8453L7.65399 15.6654L1.40399 12.4154L1 12.2053V11.75V4.25V3.79466L1.40399 3.58459L7.65399 0.334591L8 0.154663ZM2.5 11.2947V5.44058L7.25 7.81559V13.7647L2.5 11.2947ZM8.75 13.7647L13.5 11.2947V5.44056L8.75 7.81556V13.7647ZM8 1.84534L12.5766 4.22519L7.99998 6.51352L3.42335 4.2252L8 1.84534Z"
      fill="currentColor"
    />
    <circle cx="8" cy="8" r="2" fill="currentColor" opacity="0.6" />
  </svg>
);

export interface AttachmentButtonProps {
  fileType: 'csv' | 'duckdb';
  onFileSelect: (file: File) => void;
  disabled?: boolean;
  accept: string;
  tooltip: string;
  className?: string;
}

export const AttachmentButton = React.forwardRef<
  HTMLButtonElement,
  AttachmentButtonProps
>(({ fileType, onFileSelect, disabled = false, accept, tooltip, className }, ref) => {
  const fileInputRef = React.useRef<HTMLInputElement>(null);

  const handleClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    fileInputRef.current?.click();
  };

  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      onFileSelect(file);
    }
    // Reset the input value to allow selecting the same file again
    event.target.value = '';
  };

  const getIcon = () => {
    switch (fileType) {
      case 'csv':
        return <CsvIcon size={16} />;
      case 'duckdb':
        return <DuckDbIcon size={16} />;
      default:
        return <CsvIcon size={16} />;
    }
  };

  const getVariantStyles = () => {
    switch (fileType) {
      case 'csv':
        return "hover:bg-green-50 hover:border-green-200 hover:text-green-700 dark:hover:bg-green-950 dark:hover:border-green-800 dark:hover:text-green-300";
      case 'duckdb':
        return "hover:bg-blue-50 hover:border-blue-200 hover:text-blue-700 dark:hover:bg-blue-950 dark:hover:border-blue-800 dark:hover:text-blue-300";
      default:
        return "";
    }
  };

  return (
    <>
      <Button
        ref={ref}
        className={cn(
          "rounded-full p-1.5 h-fit border dark:border-zinc-600 transition-colors duration-200",
          getVariantStyles(),
          className
        )}
        onClick={handleClick}
        disabled={disabled}
        title={tooltip}
        variant="outline"
        size="icon"
      >
        {getIcon()}
      </Button>
      
      <input
        ref={fileInputRef}
        type="file"
        accept={accept}
        className="hidden"
        onChange={handleFileChange}
      />
    </>
  );
});

AttachmentButton.displayName = "AttachmentButton";