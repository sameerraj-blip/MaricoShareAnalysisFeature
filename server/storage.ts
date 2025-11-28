import { SessionData } from "./shared/schema.js";
import { randomUUID } from "crypto";

// Update the storage interface for our application
export interface IStorage {
  // Session management
  createSession(data: SessionData): string;
  getSession(sessionId: string): SessionData | undefined;
  deleteSession(sessionId: string): void;
}

export class MemStorage implements IStorage {
  private sessions: Map<string, SessionData>;

  constructor() {
    this.sessions = new Map();
  }

  createSession(data: SessionData): string {
    const sessionId = randomUUID();
    this.sessions.set(sessionId, data);
    return sessionId;
  }

  getSession(sessionId: string): SessionData | undefined {
    return this.sessions.get(sessionId);
  }

  deleteSession(sessionId: string): void {
    this.sessions.delete(sessionId);
  }
}

export const storage = new MemStorage();
