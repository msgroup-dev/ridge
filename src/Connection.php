<?php
/**
 * This file is part of PHPinnacle/Ridge.
 *
 * (c) PHPinnacle Team <dev@phpinnacle.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace PHPinnacle\Ridge;

use Amp\Socket\DnsSocketConnector;
use Amp\Socket\RetrySocketConnector;
use Evenement\EventEmitterTrait;
use PHPinnacle\Ridge\Exception\ConnectionException;
use function Amp\async;
use Amp\Socket\ConnectContext;
use Revolt\EventLoop;
use Amp\Socket\Socket;
use PHPinnacle\Ridge\Protocol\AbstractFrame;
use function Amp\now;

final class Connection
{
    use EventEmitterTrait;

    public const EVENT_CLOSE = 'close';

    private Parser $parser;

    private ?Socket $socket = null;

    private bool $socketClosedExpectedly = false;

    /**
     * @var array<int, array<class-string<AbstractFrame>, list<callable(AbstractFrame):bool>>>
     */
    private array $callbacks = [];

    private float $lastWrite = 0;

    private float $lastRead = 0;

    private ?string $heartbeatWatcherId = null;

    public function __construct(
        private readonly string $uri
    ) {
        $this->parser = new Parser;
    }

    public function connected(): bool
    {
        return $this->socket !== null && $this->socket->isClosed() === false;
    }

    /**
     * @throws \PHPinnacle\Ridge\Exception\ConnectionException
     */
    public function write(Buffer $payload): void
    {
        $this->lastWrite = now();

        if ($this->socket !== null) {
            try {
                $this->socket->write($payload->flush());
                return;
            } catch (\Throwable $throwable) {
                throw ConnectionException::writeFailed($throwable);
            }
        }

        throw ConnectionException::socketClosed();
    }

    /**
     * @throws \PHPinnacle\Ridge\Exception\ConnectionException
     */
    public function method(int $channel, Buffer $payload): void
    {
        $this->write((new Buffer)
            ->appendUint8(1)
            ->appendUint16($channel)
            ->appendUint32($payload->size())
            ->append($payload)
            ->appendUint8(206)
        );
    }

    /**
     * @psalm-param class-string<AbstractFrame> $frame
     */
    public function subscribe(int $channel, string $frame, callable $callback): void
    {
        $this->callbacks[$channel][$frame][] = $callback;
    }

    public function cancel(int $channel): void
    {
        unset($this->callbacks[$channel]);
    }

    /**
     * @throws \PHPinnacle\Ridge\Exception\ConnectionException
     */
    public function open(int $timeout, int $maxAttempts, bool $noDelay): void
    {
        $socketConnector = new DnsSocketConnector();
        $context = new ConnectContext();

        if ($maxAttempts > 0) {
            $socketConnector = new RetrySocketConnector($socketConnector, maxAttempts: $maxAttempts);
        }

        if ($timeout > 0) {
            $context = $context->withConnectTimeout($timeout);
        }

        if ($noDelay) {
            $context = $context->withTcpNoDelay();
        }

        $this->socket = $socketConnector->connect($this->uri, $context);
        $this->socketClosedExpectedly = false;
        $this->lastRead = now();

        async(
            function () {
                if ($this->socket === null) {
                    throw ConnectionException::socketClosed();
                }

                while (null !== $chunk = $this->socket->read()) {
                    $this->parser->append($chunk);

                    while ($frame = $this->parser->parse()) {
                        $class = \get_class($frame);
                        $this->lastRead = now();

                        /**
                         * @psalm-var callable(AbstractFrame):bool $callback
                         */
                        foreach ($this->callbacks[(int)$frame->channel][$class] ?? [] as $i => $callback) {

                            if (call_user_func($callback, $frame)) {
                                unset($this->callbacks[(int)$frame->channel][$class][$i]);
                            }
                        }
                    }
                }

                $this->emit(self::EVENT_CLOSE, $this->socketClosedExpectedly ? [] : [Exception\ConnectionException::lostConnection()]);
                $this->socket = null;
            }
        );
    }

    public function heartbeat(int $timeout): void
    {
        /**
         * Heartbeat interval should be timeout / 2 according to rabbitmq docs
         * @link https://www.rabbitmq.com/heartbeats.html#heartbeats-timeout
         *
         * We run the callback even more often to avoid race conditions if the loop is a bit under pressure
         * otherwise we could miss heartbeats in rare conditions
         */
        $interval = $timeout / 2;
        $this->heartbeatWatcherId = EventLoop::repeat(
            $interval / 3,
            function (string $watcherId) use ($interval, $timeout){
                $currentTime = now();

                if (null !== $this->socket) {
                    $lastWrite = $this->lastWrite ?: $currentTime;

                    $nextHeartbeat = $lastWrite + $interval;

                    if ($currentTime >= $nextHeartbeat) {
                        $this->write((new Buffer)
                            ->appendUint8(8)
                            ->appendUint16(0)
                            ->appendUint32(0)
                            ->appendUint8(206)
                        );
                    }

                    unset($lastWrite, $nextHeartbeat);
                }

                if (
                    0 !== $this->lastRead &&
                    $currentTime > ($this->lastRead + $timeout + 1000)
                )
                {
                    EventLoop::cancel($watcherId);
                }

                unset($currentTime);
            });
    }

    public function close(): void
    {
        $this->callbacks = [];

        if ($this->heartbeatWatcherId !== null) {
            EventLoop::cancel($this->heartbeatWatcherId);

            $this->heartbeatWatcherId = null;
        }

        if ($this->socket !== null) {
            $this->socketClosedExpectedly = true;
            $this->socket->close();
        }
    }
}
