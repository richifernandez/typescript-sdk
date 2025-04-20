import { IncomingMessage, ServerResponse } from "node:http";
import { Transport } from "../shared/transport.js";
import {
  isInitializeRequest,
  isJSONRPCError,
  isJSONRPCRequest,
  isJSONRPCResponse,
  JSONRPCMessage,
  JSONRPCMessageSchema,
  RequestId
} from "../types.js";
import getRawBody from "raw-body";
import contentType from "content-type";
import { randomUUID } from "node:crypto";

const MAXIMUM_MESSAGE_SIZE = "4mb";

export type StreamId = string;
export type EventId = string;

export interface EventStore {
  storeEvent(streamId: StreamId, message: JSONRPCMessage): Promise<EventId>;
  replayEventsAfter(
    lastEventId: EventId,
    opts: { send: (eventId: EventId, message: JSONRPCMessage) => Promise<void> }
  ): Promise<StreamId>;
}

export interface StreamableHTTPServerTransportOptions {
  sessionIdGenerator: (() => string) | undefined;
  onsessioninitialized?: (sessionId: string) => void;
  enableJsonResponse?: boolean;
  eventStore?: EventStore;
}

export class StreamableHTTPServerTransport implements Transport {
  private sessionIdGenerator: (() => string) | undefined;
  private _started = false;
  private _streamMapping = new Map<string, ServerResponse>();
  private _requestToStreamMapping = new Map<RequestId, string>();
  private _requestResponseMap = new Map<RequestId, JSONRPCMessage>();
  private _initialized = false;
  private _enableJsonResponse = false;
  private _standaloneSseStreamId = "_GET_stream";
  private _eventStore?: EventStore;
  private _onsessioninitialized?: (sessionId: string) => void;

  public sessionId?: string;
  public onclose?: () => void;
  public onerror?: (error: Error) => void;
  public onmessage?: (message: JSONRPCMessage) => void;

  constructor(options: StreamableHTTPServerTransportOptions) {
    this.sessionIdGenerator = options.sessionIdGenerator;
    this._enableJsonResponse = options.enableJsonResponse ?? false;
    this._eventStore = options.eventStore;
    this._onsessioninitialized = options.onsessioninitialized;
  }

  async start(): Promise<void> {
    if (this._started) throw new Error("Transport already started");
    this._started = true;
  }

  async handleRequest(
    req: IncomingMessage,
    res: ServerResponse,
    parsedBody?: unknown
  ): Promise<void> {
    switch (req.method) {
      case "POST":
        await this.handlePostRequest(req, res, parsedBody);
        break;
      case "GET":
        await this.handleGetRequest(req, res);
        break;
      case "DELETE":
        await this.handleDeleteRequest(req, res);
        break;
      default:
        await this.handleUnsupportedRequest(res);
    }
  }

  private async handleGetRequest(
    req: IncomingMessage,
    res: ServerResponse
  ): Promise<void> {
    const acceptHeader = req.headers.accept;
    if (!acceptHeader?.includes("text/event-stream")) {
      res.writeHead(406).end(
        JSON.stringify({
          jsonrpc: "2.0",
          error: {
            code: -32000,
            message: "Not Acceptable: Client must accept text/event-stream"
          },
          id: null
        })
      );
      return;
    }
    if (!this.validateSession(req, res)) return;

    if (this._eventStore) {
      const lastEventId = req.headers["last-event-id"] as string | undefined;
      if (lastEventId) {
        await this.replayEvents(lastEventId, res);
        return;
      }
    }

    const headers: Record<string, string> = {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive"
    };
    if (this.sessionId !== undefined) {
      headers["mcp-session-id"] = this.sessionId;
    }
    if (this._streamMapping.has(this._standaloneSseStreamId)) {
      res.writeHead(409).end(
        JSON.stringify({
          jsonrpc: "2.0",
          error: {
            code: -32000,
            message: "Conflict: Only one SSE stream is allowed per session"
          },
          id: null
        })
      );
      return;
    }

    res.writeHead(200, headers).flushHeaders();
    this._streamMapping.set(this._standaloneSseStreamId, res);
    res.on("close", () => {
      this._streamMapping.delete(this._standaloneSseStreamId);
    });
  }

  private async replayEvents(
    lastEventId: string,
    res: ServerResponse
  ): Promise<void> {
    if (!this._eventStore) return;
    try {
      const headers: Record<string, string> = {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache, no-transform",
        Connection: "keep-alive"
      };
      if (this.sessionId !== undefined) {
        headers["mcp-session-id"] = this.sessionId;
      }
      res.writeHead(200, headers).flushHeaders();

      const streamId = await this._eventStore.replayEventsAfter(lastEventId, {
        send: async (eventId, message) => {
          if (!this.writeSSEEvent(res, message, eventId)) {
            this.onerror?.(new Error("Failed replay events"));
            res.end();
          }
        }
      });
      this._streamMapping.set(streamId, res);
    } catch (err) {
      this.onerror?.(err as Error);
    }
  }

  private writeSSEEvent(
    res: ServerResponse,
    message: JSONRPCMessage,
    eventId?: string
  ): boolean {
    let data = "event: message\n";
    if (eventId) data += `id: ${eventId}\n`;
    data += `data: ${JSON.stringify(message)}\n\n`;
    return res.write(data);
  }

  private async handleUnsupportedRequest(
    res: ServerResponse
  ): Promise<void> {
    res.writeHead(405, { Allow: "GET, POST, DELETE" }).end(
      JSON.stringify({
        jsonrpc: "2.0",
        error: {
          code: -32000,
          message: "Method not allowed."
        },
        id: null
      })
    );
  }

  private async handlePostRequest(
    req: IncomingMessage,
    res: ServerResponse,
    parsedBody?: unknown
  ): Promise<void> {
    try {
      const acceptHeader = req.headers.accept;
      if (!acceptHeader?.includes("application/json")) {
        res.writeHead(406).end(
          JSON.stringify({
            jsonrpc: "2.0",
            error: {
              code: -32000,
              message: "Not Acceptable: Client must accept application/json"
            },
            id: null
          })
        );
        return;
      }

      const ct = req.headers["content-type"];
      if (!ct || !ct.includes("application/json")) {
        res.writeHead(415).end(
          JSON.stringify({
            jsonrpc: "2.0",
            error: {
              code: -32000,
              message:
                "Unsupported Media Type: Content-Type must be application/json"
            },
            id: null
          })
        );
        return;
      }

      let rawMessage;
      if (parsedBody !== undefined) {
        rawMessage = parsedBody;
      } else {
        const parsedCt = contentType.parse(ct);
        const body = await getRawBody(req, {
          limit: MAXIMUM_MESSAGE_SIZE,
          encoding: parsedCt.parameters.charset ?? "utf-8"
        });
        rawMessage = JSON.parse(body.toString());
      }

      const messages: JSONRPCMessage[] = Array.isArray(rawMessage)
        ? rawMessage.map((m) => JSONRPCMessageSchema.parse(m))
        : [JSONRPCMessageSchema.parse(rawMessage)];

      const isInit = messages.some(isInitializeRequest);
      if (isInit) {
        if (this._initialized && this.sessionId !== undefined) {
          res.writeHead(400).end(
            JSON.stringify({
              jsonrpc: "2.0",
              error: {
                code: -32600,
                message: "Invalid Request: Server already initialized"
              },
              id: null
            })
          );
          return;
        }
        if (messages.length > 1) {
          res.writeHead(400).end(
            JSON.stringify({
              jsonrpc: "2.0",
              error: {
                code: -32600,
                message:
                  "Invalid Request: Only one initialization request is allowed"
              },
              id: null
            })
          );
          return;
        }
        this.sessionId = this.sessionIdGenerator?.();
        this._initialized = true;
        if (this.sessionId && this._onsessioninitialized) {
          this._onsessioninitialized(this.sessionId);
        }
      }

      if (!isInit && !this.validateSession(req, res)) {
        return;
      }

      const hasRequests = messages.some(isJSONRPCRequest);
      if (!hasRequests) {
        res.writeHead(202).end();
        for (const m of messages) this.onmessage?.(m);
      } else {
        const streamId = randomUUID();
        if (!this._enableJsonResponse) {
          const headers: Record<string, string> = {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            Connection: "keep-alive"
          };
          if (this.sessionId !== undefined) {
            headers["mcp-session-id"] = this.sessionId;
          }
          res.writeHead(200, headers);
        }
        for (const m of messages) {
          if (isJSONRPCRequest(m)) {
            this._streamMapping.set(streamId, res);
            this._requestToStreamMapping.set(m.id, streamId);
          }
        }
        res.on("close", () =>
          this._streamMapping.delete(streamId)
        );
        for (const m of messages) this.onmessage?.(m);
      }
    } catch (err) {
      res.writeHead(400).end(
        JSON.stringify({
          jsonrpc: "2.0",
          error: {
            code: -32700,
            message: "Parse error",
            data: String(err)
          },
          id: null
        })
      );
      this.onerror?.(err as Error);
    }
  }

  private async handleDeleteRequest(
    req: IncomingMessage,
    res: ServerResponse
  ): Promise<void> {
    if (!this.validateSession(req, res)) return;
    await this.close();
    res.writeHead(200).end();
  }

  private validateSession(
    req: IncomingMessage,
    res: ServerResponse
  ): boolean {
    return true
  }

  async close(): Promise<void> {
    this._streamMapping.forEach((r) => r.end());
    this._streamMapping.clear();
    this._requestResponseMap.clear();
    this.onclose?.();
  }

  async send(
    message: JSONRPCMessage,
    options?: { relatedRequestId?: RequestId }
  ): Promise<void> {
    let requestId = options?.relatedRequestId;
    if (isJSONRPCResponse(message) || isJSONRPCError(message)) {
      requestId = message.id;
    }

    // standalone SSE
    if (requestId === undefined) {
      if (isJSONRPCResponse(message) || isJSONRPCError(message)) {
        throw new Error(
          "Cannot send a response on a standalone SSE stream unless resuming a previous client request"
        );
      }
      const sse = this._streamMapping.get(this._standaloneSseStreamId);
      if (!sse) return;
      let eventId: string | undefined;
      if (this._eventStore) {
        eventId = await this._eventStore.storeEvent(
          this._standaloneSseStreamId,
          message
        );
      }
      this.writeSSEEvent(sse, message, eventId);
      return;
    }

    // response-per-request
    const streamId = this._requestToStreamMapping.get(requestId);
    if (!streamId) {
      throw new Error(`No connection established for request ID: ${requestId}`);
    }
    const response = this._streamMapping.get(streamId)!;

    if (!this._enableJsonResponse) {
      let eventId: string | undefined;
      if (this._eventStore) {
        eventId = await this._eventStore.storeEvent(streamId, message);
      }
      // SSE style
      this.writeSSEEvent(response, message, eventId);
    }

    if (isJSONRPCResponse(message)) {
      this._requestResponseMap.set(requestId, message);
      const related = Array.from(this._requestToStreamMapping.entries())
        .filter(([, sid]) => this._streamMapping.get(sid) === response)
        .map(([id]) => id);

      if (related.every((id) => this._requestResponseMap.has(id))) {
        if (this._enableJsonResponse) {
          const headers: Record<string, string> = {
            "Content-Type": "application/json"
          };
          if (this.sessionId !== undefined) {
            headers["mcp-session-id"] = this.sessionId;
          }
          response.writeHead(200, headers);
          const out = related.map((id) => this._requestResponseMap.get(id)!);
          response.end(out.length === 1 ? JSON.stringify(out[0]) : JSON.stringify(out));
        } else {
          response.end();
        }
        for (const id of related) {
          this._requestResponseMap.delete(id);
          this._requestToStreamMapping.delete(id);
        }
      }
    }
  }
}