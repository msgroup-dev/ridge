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

use Amp\CompositeException;
use Revolt\EventLoop;
use Evenement\EventEmitterTrait;
use PHPinnacle\Ridge\Exception\ChannelException;
use PHPinnacle\Ridge\Exception\ConnectionException;
use function Amp\async;
use Amp\DeferredFuture;
use Amp\Future;

final class Client
{
    use EventEmitterTrait;

    public const EVENT_CLOSE = 'close';

    private const STATE_NOT_CONNECTED = 0;
    private const STATE_CONNECTING = 1;
    private const STATE_CONNECTED = 2;
    private const STATE_DISCONNECTING = 3;

    private const CONNECTION_MONITOR_INTERVAL = 5000;

    private int $state = self::STATE_NOT_CONNECTED;

    /**
     * @var array<int, Channel>
     */
    private array $channels = [];

    private int $nextChannelId = 1;

    private Connection $connection;

    private Properties $properties;

    private ?string $connectionMonitorWatcherId = null;

    private CommandWaitQueue $commandWaitQueue;

    public function __construct(
        private readonly Config $config
    ) {
        $this->commandWaitQueue = new CommandWaitQueue();
        $this->on(self::EVENT_CLOSE, function (\Throwable $exception = null) {
            if ($exception !== null) {
                foreach ($this->channels as $channel) {
                    $channel->forceClose($exception);
                }
                $this->channels = [];
                $this->commandWaitQueue->cancel($exception);
            }
        });
    }

    public static function create(string $dsn): self
    {
        return new self(Config::parse($dsn));
    }

    /**
     * @throws \PHPinnacle\Ridge\Exception\ClientException
     */
    public function properties(): Properties
    {
        if ($this->state !== self::STATE_CONNECTED) {
            throw Exception\ClientException::notConnected();
        }

        return $this->properties;
    }

    /**
     * @throws \PHPinnacle\Ridge\Exception\ClientException
     */
    public function connect(): void
    {
        if ($this->state !== self::STATE_NOT_CONNECTED) {
            throw Exception\ClientException::alreadyConnected();
        }

        $this->state = self::STATE_CONNECTING;

        $this->connection = new Connection($this->config->uri());
        $this->connection->once(Connection::EVENT_CLOSE, function(\Throwable $exception = null) {
            $this->state = self::STATE_NOT_CONNECTED;
            $this->emit(self::EVENT_CLOSE, [$exception]);
        });

        $this->connection->open(
            $this->config->timeout,
            $this->config->tcpAttempts,
            $this->config->tcpNoDelay
        );

        $buffer = new Buffer;
        $buffer
            ->append('AMQP')
            ->appendUint8(0)
            ->appendUint8(0)
            ->appendUint8(9)
            ->appendUint8(1);

        $this->connection->write($buffer);

        $this->connectionStart();
        $this->connectionTune();
        $this->connectionOpen();

        async(
            function () {
                /** @var Protocol\ConnectionCloseFrame $frame */
                [$frame] = Future\await([$this->await(Protocol\ConnectionCloseFrame::class)]);
                $buffer = new Buffer;
                $buffer
                    ->appendUint8(1)
                    ->appendUint16(0)
                    ->appendUint32(4)
                    ->appendUint16(10)
                    ->appendUint16(51)
                    ->appendUint8(206);

                $this->connection->write($buffer);
                $this->connection->close();

                $exception = Exception\ClientException::connectionClosed($frame);

                $this->disableConnectionMonitor();

                $this->emit(self::EVENT_CLOSE, [$exception]);
            }
        );

        $this->state = self::STATE_CONNECTED;

        $this->connectionMonitorWatcherId = EventLoop::repeat(
            self::CONNECTION_MONITOR_INTERVAL,
            function(): void
            {
                if($this->connection->connected() === false) {
                    $this->state = self::STATE_NOT_CONNECTED;
                    $this->emit(self::EVENT_CLOSE, [Exception\ClientException::disconnected()]);
                }
            }
        );
    }

    /**
     * @throws \PHPinnacle\Ridge\Exception\ClientException
     */
    public function disconnect(int $code = 0, string $reason = ''): void
    {
        $this->disableConnectionMonitor();

        try {
            if (\in_array($this->state, [self::STATE_NOT_CONNECTED, self::STATE_DISCONNECTING])) {
                return;
            }

            if ($this->state !== self::STATE_CONNECTED) {
                throw Exception\ClientException::notConnected();
            }

            if($this->connectionMonitorWatcherId !== null){
                EventLoop::cancel($this->connectionMonitorWatcherId);

                $this->connectionMonitorWatcherId = null;
            }

            $this->state = self::STATE_DISCONNECTING;

            if ($code === 0) {
                $promises = [];

                foreach ($this->channels as $channel) {
                    $promises[] = async(fn() => $channel->close($code, $reason));
                }

                // Gracefully continue to close connection even if closing channels fails
                Future\awaitAll($promises);
                $this->channels = [];
            }

            $this->connectionClose($code, $reason);

            $this->connection->close();
        }
        finally
        {
            $this->state = self::STATE_NOT_CONNECTED;
        }
    }

    /**
     * @throws \PHPinnacle\Ridge\Exception\ClientException
     */
    public function channel(): Channel
    {
        if ($this->state !== self::STATE_CONNECTED) {
            throw Exception\ClientException::notConnected();
        }

        try {
            $id = $this->findChannelId();
            $channel = new Channel($id, $this->connection, $this->properties);

            $this->channels[$id] = $channel;

            $channel->open();
            $channel->qos($this->config->qosSize, $this->config->qosCount, $this->config->qosGlobal);

            async(function () use ($id) {
                try {
                    $frame = yield Future\awaitFirst([
                        $this->await(Protocol\ChannelCloseFrame::class, $id),
                        $this->await(Protocol\ChannelCloseOkFrame::class, $id)
                    ]);

                    $channel = $this->channels[$id];
                    unset($this->channels[$id]);

                    if ($frame instanceof Protocol\ChannelCloseFrame) {
                        $buffer = new Buffer;
                        $buffer
                            ->appendUint8(1)
                            ->appendUint16($id)
                            ->appendUint32(4)
                            ->appendUint16(20)
                            ->appendUint16(41)
                            ->appendUint8(206);

                        $this->connection->write($buffer);
                        $channel->forceClose(
                            new ChannelException(sprintf("Channel closed: %s", $frame->replyText))
                        );
                    }
                } catch (CompositeException) {
                    // There was an error when waiting for those frames
                    // We should not unset the channel here because the client will clean them up
                    // with the correct error when the connection close event triggers
                }

                $this->connection->cancel($id);
            });

            return $channel;
        } catch(ConnectionException $exception) {
            $this->state = self::STATE_NOT_CONNECTED;

            throw $exception;
        } catch (\Throwable $error) {
            throw Exception\ClientException::unexpectedResponse($error);
        }
    }

    public function isConnected(): bool
    {
        return $this->state === self::STATE_CONNECTED && $this->connection->connected();
    }

    /**
     * @throws \PHPinnacle\Ridge\Exception\ClientException
     */
    private function connectionStart(): void
    {
        /** @var Protocol\ConnectionStartFrame $start */
        [$start] = Future\await([$this->await(Protocol\ConnectionStartFrame::class)]);

        if (!\str_contains($start->mechanisms, 'AMQPLAIN')) {
            throw Exception\ClientException::notSupported($start->mechanisms);
        }

        $this->properties = Properties::create($start->serverProperties);

        $buffer = new Buffer;
        $buffer
            ->appendTable([
                'LOGIN' => $this->config->user,
                'PASSWORD' => $this->config->pass,
            ])
            ->discard(4);

        $frameBuffer = new Buffer;
        $frameBuffer
            ->appendUint16(10)
            ->appendUint16(11)
            ->appendTable([])
            ->appendString('AMQPLAIN')
            ->appendText((string)$buffer)
            ->appendString('en_US');

        $this->connection->method(0, $frameBuffer);
    }

    private function connectionTune(): void
    {
        /** @var Protocol\ConnectionTuneFrame $tune */
        [$tune] = Future\await([$this->await(Protocol\ConnectionTuneFrame::class)]);

        $heartbeatTimeout = $this->config->heartbeat;

        if ($heartbeatTimeout !== 0) {
            $heartbeatTimeout = \min($heartbeatTimeout, $tune->heartbeat * 1000);
        }

        $maxChannel = \min($this->config->maxChannel, $tune->channelMax);
        $maxFrame = \min($this->config->maxFrame, $tune->frameMax);

        $buffer = new Buffer;
        $buffer
            ->appendUint8(1)
            ->appendUint16(0)
            ->appendUint32(12)
            ->appendUint16(10)
            ->appendUint16(31)
            ->appendInt16($maxChannel)
            ->appendInt32($maxFrame)
            ->appendInt16((int) ($heartbeatTimeout / 1000))
            ->appendUint8(206);

        $this->connection->write($buffer);

        $this->properties->tune($maxChannel, $maxFrame);

        if ($heartbeatTimeout > 0) {
            $this->connection->heartbeat($heartbeatTimeout);
        }
    }

    private function connectionOpen(): Protocol\ConnectionOpenOkFrame
    {
        $vhost = $this->config->vhost;
        $capabilities = '';
        $insist = false;

        $buffer = new Buffer;
        $buffer
            ->appendUint8(1)
            ->appendUint16(0)
            ->appendUint32(7 + \strlen($vhost) + \strlen($capabilities))
            ->appendUint16(10)
            ->appendUint16(40)
            ->appendString($vhost)
            ->appendString($capabilities) // TODO: process server capabilities
            ->appendBits([$insist])
            ->appendUint8(206);

        $this->connection->write($buffer);

        [$result] = Future\await([$this->await(Protocol\ConnectionOpenOkFrame::class)]);

        return $result;
    }

    private function connectionClose(int $code, string $reason): void
    {
        $buffer = new Buffer;
        $buffer
            ->appendUint8(1)
            ->appendUint16(0)
            ->appendUint32(11 + \strlen($reason))
            ->appendUint16(10)
            ->appendUint16(50)
            ->appendInt16($code)
            ->appendString($reason)
            ->appendInt16(0)
            ->appendInt16(0)
            ->appendUint8(206);

        $this->connection->write($buffer);

        Future\await([$this->await(Protocol\ConnectionCloseOkFrame::class)]);
    }

    private function findChannelId(): int
    {
        /** first check in range [next, max] ... */
        for ($id = $this->nextChannelId; $id <= $this->config->maxChannel; ++$id) {
            if (!isset($this->channels[$id])) {
                $this->nextChannelId = $id + 1;

                return $id;
            }
        }

        /** then check in range [min, next) ... */
        for ($id = 1; $id < $this->nextChannelId; ++$id) {
            if (!isset($this->channels[$id])) {
                $this->nextChannelId = $id + 1;

                return $id;
            }
        }

        throw Exception\ClientException::noChannelsAvailable();
    }

    /**
     * @template T of Protocol\AbstractFrame
     * @psalm-param class-string<T> $frame
     * @psalm-return Future<T>
     */
    private function await(string $frame, int $channel = 0): Future
    {
        /** @psalm-var DeferredFuture<T> $deferred */
        $deferred = new DeferredFuture;
        $this->commandWaitQueue->add($deferred);

        $this->connection->subscribe(
            $channel,
            $frame,
            static function (Protocol\AbstractFrame $frame) use ($deferred) {
                /** @psalm-var T $frame */
                $deferred->complete($frame);

                return true;
            }
        );

        return $deferred->getFuture();
    }

    private function disableConnectionMonitor(): void {
        if($this->connectionMonitorWatcherId !== null) {

            EventLoop::cancel($this->connectionMonitorWatcherId);

            $this->connectionMonitorWatcherId = null;
        }
    }
}
