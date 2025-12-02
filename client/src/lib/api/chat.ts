import { API_BASE_URL } from "@/lib/config";
import { getUserEmail } from "@/utils/userStorage";
import { ChatResponse, ThinkingStep } from "@/shared/schema";

export interface StreamChatCallbacks {
  onThinkingStep?: (step: ThinkingStep) => void;
  onResponse?: (response: ChatResponse) => void;
  onError?: (error: Error) => void;
  onDone?: () => void;
}

export async function streamChatRequest(
  sessionId: string,
  message: string,
  chatHistory: Array<{ role: string; content: string }>,
  callbacks: StreamChatCallbacks,
  signal?: AbortSignal,
  targetTimestamp?: number
): Promise<void> {
  const userEmail = getUserEmail();
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };

  if (userEmail) {
    headers["X-User-Email"] = userEmail;
  }

  try {
    console.log("🌐 Starting SSE stream to:", `${API_BASE_URL}/api/chat/stream`);
    const response = await fetch(`${API_BASE_URL}/api/chat/stream`, {
      method: "POST",
      headers,
      credentials: "include",
      body: JSON.stringify({
        sessionId,
        message,
        chatHistory,
        targetTimestamp,
      }),
      signal,
    });

    console.log("📡 SSE response status:", response.status, response.statusText);

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    if (!response.body) {
      throw new Error("Response body is null");
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          break;
        }

        buffer += decoder.decode(value, { stream: true });

        let messageEnd;
        while ((messageEnd = buffer.indexOf("\n\n")) !== -1) {
          const messageChunk = buffer.substring(0, messageEnd);
          buffer = buffer.substring(messageEnd + 2);

          let eventType = "message";
          let data = "";

          const lines = messageChunk.split("\n");
          for (const line of lines) {
            if (line.startsWith("event: ")) {
              eventType = line.substring(7).trim();
            } else if (line.startsWith("data: ")) {
              data = line.substring(6).trim();
            }
          }

          if (data) {
            try {
              const parsed = JSON.parse(data);
              console.log("📡 SSE event received:", eventType, parsed);
              dispatchEvent(eventType, parsed, callbacks);
            } catch (parseError) {
              console.error("Error parsing SSE data:", parseError, data);
            }
          }
        }
      }

      if (buffer.trim()) {
        handleTrailingBuffer(buffer, callbacks);
      }
    } finally {
      reader.releaseLock();
    }
  } catch (error: any) {
    if (error.name === "AbortError" || signal?.aborted) {
      console.log("🚫 Stream request was cancelled");
      return;
    }

    if (callbacks.onError) {
      callbacks.onError(error instanceof Error ? error : new Error(String(error)));
    } else {
      throw error;
    }
  }
}

function dispatchEvent(
  eventType: string,
  payload: unknown,
  callbacks: StreamChatCallbacks
) {
  switch (eventType) {
    case "thinking":
      callbacks.onThinkingStep?.(payload as ThinkingStep);
      break;
    case "response":
      callbacks.onResponse?.(payload as ChatResponse);
      break;
    case "error":
      callbacks.onError?.(
        new Error((payload as { message?: string })?.message || "Unknown error")
      );
      break;
    case "done":
      callbacks.onDone?.();
      break;
    default:
      break;
  }
}

function handleTrailingBuffer(buffer: string, callbacks: StreamChatCallbacks) {
  let eventType = "message";
  let data = "";

  const lines = buffer.split("\n");
  for (const line of lines) {
    if (line.startsWith("event: ")) {
      eventType = line.substring(7).trim();
    } else if (line.startsWith("data: ")) {
      data = line.substring(6).trim();
    }
  }

  if (!data) {
    return;
  }

  try {
    const parsed = JSON.parse(data);
    console.log("📡 Final SSE event:", eventType, parsed);
    dispatchEvent(eventType, parsed, callbacks);
  } catch (parseError) {
    console.error("Error parsing final SSE data:", parseError);
  }
}

export interface DataOpsResponse {
  answer: string;
  preview?: Record<string, any>[];
  summary?: any[];
  saved?: boolean;
}

export interface StreamDataOpsCallbacks {
  onThinkingStep?: (step: ThinkingStep) => void;
  onResponse?: (response: DataOpsResponse) => void;
  onError?: (error: Error) => void;
  onDone?: () => void;
}

export async function streamDataOpsChatRequest(
  sessionId: string,
  message: string,
  chatHistory: Array<{ role: string; content: string }>,
  callbacks: StreamDataOpsCallbacks,
  signal?: AbortSignal,
  targetTimestamp?: number,
  dataOpsMode?: boolean
): Promise<void> {
  const userEmail = getUserEmail();
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };

  if (userEmail) {
    headers["X-User-Email"] = userEmail;
  }

  try {
    console.log("🌐 Starting Data Ops SSE stream to:", `${API_BASE_URL}/api/data-ops/chat/stream`);
    const response = await fetch(`${API_BASE_URL}/api/data-ops/chat/stream`, {
      method: "POST",
      headers,
      credentials: "include",
      body: JSON.stringify({
        sessionId,
        message,
        chatHistory,
        targetTimestamp,
        dataOpsMode,
      }),
      signal,
    });

    console.log("📡 Data Ops SSE response status:", response.status, response.statusText);

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    if (!response.body) {
      throw new Error("Response body is null");
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          break;
        }

        buffer += decoder.decode(value, { stream: true });

        let messageEnd;
        while ((messageEnd = buffer.indexOf("\n\n")) !== -1) {
          const messageChunk = buffer.substring(0, messageEnd);
          buffer = buffer.substring(messageEnd + 2);

          let eventType = "message";
          let data = "";

          const lines = messageChunk.split("\n");
          for (const line of lines) {
            if (line.startsWith("event: ")) {
              eventType = line.substring(7).trim();
            } else if (line.startsWith("data: ")) {
              data = line.substring(6).trim();
            }
          }

          if (data) {
            try {
              const parsed = JSON.parse(data);
              console.log("📡 Data Ops SSE event received:", eventType, parsed);
              dispatchDataOpsEvent(eventType, parsed, callbacks);
            } catch (parseError) {
              console.error("Error parsing Data Ops SSE data:", parseError, data);
            }
          }
        }
      }

      if (buffer.trim()) {
        handleDataOpsTrailingBuffer(buffer, callbacks);
      }
    } finally {
      reader.releaseLock();
    }
  } catch (error: any) {
    if (error.name === "AbortError" || signal?.aborted) {
      console.log("🚫 Data Ops stream request was cancelled");
      return;
    }

    if (callbacks.onError) {
      callbacks.onError(error instanceof Error ? error : new Error(String(error)));
    } else {
      throw error;
    }
  }
}

function dispatchDataOpsEvent(
  eventType: string,
  payload: unknown,
  callbacks: StreamDataOpsCallbacks
) {
  switch (eventType) {
    case "thinking":
      callbacks.onThinkingStep?.(payload as ThinkingStep);
      break;
    case "response":
      callbacks.onResponse?.(payload as DataOpsResponse);
      break;
    case "error":
      callbacks.onError?.(
        new Error((payload as { message?: string })?.message || "Unknown error")
      );
      break;
    case "done":
      callbacks.onDone?.();
      break;
    default:
      break;
  }
}

function handleDataOpsTrailingBuffer(buffer: string, callbacks: StreamDataOpsCallbacks) {
  let eventType = "message";
  let data = "";

  const lines = buffer.split("\n");
  for (const line of lines) {
    if (line.startsWith("event: ")) {
      eventType = line.substring(7).trim();
    } else if (line.startsWith("data: ")) {
      data = line.substring(6).trim();
    }
  }

  if (!data) {
    return;
  }

  try {
    const parsed = JSON.parse(data);
    console.log("📡 Final Data Ops SSE event:", eventType, parsed);
    dispatchDataOpsEvent(eventType, parsed, callbacks);
  } catch (parseError) {
    console.error("Error parsing final Data Ops SSE data:", parseError);
  }
}


