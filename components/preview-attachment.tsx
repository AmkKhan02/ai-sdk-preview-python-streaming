import type { Attachment } from "ai";
import { Button } from "./ui/button";

import { LoaderIcon } from "./icons";

export const PreviewAttachment = ({
  attachment,
  setAttachment,
  isUploading = false,
}: {
  attachment: Attachment;
  setAttachment: (file: undefined) => void;
  isUploading?: boolean;
}) => {
  const { name, url, contentType } = attachment;

  return (
    <div className="relative w-fit">
      <Button
        variant="ghost"
        className="absolute -top-2 -right-2 rounded-full p-1 h-fit"
        onClick={() => setAttachment(undefined)}
      >
        x
      </Button>
      <div className="flex flex-col gap-2">
        <div className="w-20 aspect-video bg-muted rounded-md relative flex flex-col items-center justify-center">
          {contentType ? (
            contentType.startsWith("image") ? (
              // NOTE: it is recommended to use next/image for images
              // eslint-disable-next-line @next/next/no-img-element
              <img
                key={url}
                src={url}
                alt={name ?? "An image attachment"}
                className="rounded-md size-full object-cover"
              />
            ) : (
              <div className="" />
            )
          ) : (
            <div className="" />
          )}

          {isUploading && (
            <div className="animate-spin absolute text-zinc-500">
              <LoaderIcon />
            </div>
          )}
        </div>
        <div className="text-xs text-zinc-500 max-w-16 truncate">{name}</div>
      </div>
    </div>
  );
};
