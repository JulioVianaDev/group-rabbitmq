import * as amqplib from 'amqplib';
import { EventEmitter } from 'events';

export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'reconnecting';

export interface ConnectionManagerOptions {
  amqpUrl: string;
  /** Max reconnect attempts before giving up. Default: Infinity */
  maxRetries?: number;
  /** Base delay in ms for exponential backoff. Default: 500 */
  retryDelayMs?: number;
  /** Max delay cap in ms. Default: 30_000 */
  maxRetryDelayMs?: number;
}

/**
 * ConnectionManager wraps amqplib.connect() with automatic reconnection.
 *
 * Events:
 *   "connected"     → (connection: ChannelModel)
 *   "disconnected"  → (err?: Error)
 *   "reconnecting"  → (attempt: number, delayMs: number)
 *   "failed"        → (err: Error) — gave up after maxRetries
 *
 * Usage:
 *   const mgr = new ConnectionManager({ amqpUrl: 'amqp://localhost' });
 *   mgr.on('connected', (conn) => setupChannels(conn));
 *   await mgr.connect();
 */
export class ConnectionManager extends EventEmitter {
  private connection: amqplib.ChannelModel | null = null;
  private state: ConnectionState = 'disconnected';
  private retryCount = 0;
  private shuttingDown = false;

  private readonly maxRetries: number;
  private readonly retryDelayMs: number;
  private readonly maxRetryDelayMs: number;

  constructor(private readonly options: ConnectionManagerOptions) {
    super();
    this.maxRetries = options.maxRetries ?? Infinity;
    this.retryDelayMs = options.retryDelayMs ?? 500;
    this.maxRetryDelayMs = options.maxRetryDelayMs ?? 30_000;
  }

  get currentConnection(): amqplib.ChannelModel | null {
    return this.connection;
  }

  get currentState(): ConnectionState {
    return this.state;
  }

  /**
   * Establish the initial connection. Rejects only if the very first attempt
   * fails AND maxRetries is 0.
   */
  async connect(): Promise<amqplib.ChannelModel> {
    this.shuttingDown = false;
    return this.attemptConnect();
  }

  /**
   * Cleanly close the connection and stop any reconnection attempts.
   */
  async disconnect(): Promise<void> {
    this.shuttingDown = true;
    if (this.connection) {
      await this.connection.close().catch(() => undefined);
      this.connection = null;
    }
    this.state = 'disconnected';
  }

  // ─── Private ──────────────────────────────────────────────────────────────

  private async attemptConnect(): Promise<amqplib.ChannelModel> {
    this.state = this.retryCount === 0 ? 'connecting' : 'reconnecting';

    try {
      const conn = await amqplib.connect(this.options.amqpUrl);
      this.connection = conn;
      this.state = 'connected';
      this.retryCount = 0;

      // Watch for unexpected closure
      conn.on('error', (err: Error) => {
        console.error('[group-rabbitmq:connection] Error:', err.message);
      });

      conn.on('close', () => {
        if (!this.shuttingDown) {
          this.state = 'disconnected';
          this.emit('disconnected');
          this.scheduleReconnect();
        }
      });

      this.emit('connected', conn);
      return conn;
    } catch (err) {
      return this.handleConnectError(err as Error);
    }
  }

  private async handleConnectError(err: Error): Promise<amqplib.ChannelModel> {
    this.retryCount++;

    if (this.retryCount > this.maxRetries) {
      this.state = 'disconnected';
      this.emit('failed', err);
      throw err;
    }

    const delay = this.calcDelay(this.retryCount);
    console.warn(
      `[group-rabbitmq:connection] Connect failed (attempt ${this.retryCount}). ` +
      `Retrying in ${delay}ms... ${err.message}`
    );
    this.emit('reconnecting', this.retryCount, delay);

    await sleep(delay);
    return this.attemptConnect();
  }

  private scheduleReconnect(): void {
    if (this.shuttingDown) return;

    const delay = this.calcDelay(this.retryCount + 1);
    console.warn(
      `[group-rabbitmq:connection] Connection lost. Reconnecting in ${delay}ms...`
    );
    this.emit('reconnecting', this.retryCount + 1, delay);

    setTimeout(() => {
      if (!this.shuttingDown) {
        this.attemptConnect().catch((err) => {
          this.emit('failed', err);
        });
      }
    }, delay);
  }

  private calcDelay(attempt: number): number {
    const exp = Math.min(attempt - 1, 10);
    const jitter = Math.random() * this.retryDelayMs * 0.2;
    return Math.min(
      Math.floor(this.retryDelayMs * Math.pow(2, exp) + jitter),
      this.maxRetryDelayMs
    );
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
