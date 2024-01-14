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

use function Amp\async;

final class MessageReceiver
{
    public const
        STATE_WAIT = 0,
        STATE_HEAD = 1,
        STATE_BODY = 2;

    private readonly Buffer $buffer;

    private int $state = self::STATE_WAIT;

    private int $remaining = 0;

    /**
     * @var callable[]
     */
    private array $callbacks = [];

    private ?Protocol\BasicDeliverFrame $deliver = null;

    private ?Protocol\BasicReturnFrame $return = null;

    private ?Protocol\ContentHeaderFrame $header = null;

    public function __construct(
        private readonly Channel $channel,
        private readonly Connection $connection
    ) {
        $this->buffer = new Buffer;
    }

    public function start(): void
    {
        $this->onFrame(Protocol\BasicReturnFrame::class, $this->receiveReturn(...));
        $this->onFrame(Protocol\BasicDeliverFrame::class, $this->receiveDeliver(...));
        $this->onFrame(Protocol\ContentHeaderFrame::class, $this->receiveHeader(...));
        $this->onFrame(Protocol\ContentBodyFrame::class, $this->receiveBody(...));
    }

    public function stop(): void
    {
        $this->callbacks = [];
    }

    public function onMessage(callable $callback): void
    {
        $this->callbacks[] = $callback;
    }

    /**
     * @psalm-param class-string<Protocol\AbstractFrame> $frame
     */
    public function onFrame(string $frame, callable $callback): void
    {
        $this->connection->subscribe($this->channel->id(), $frame, $callback);
    }

    public function receiveReturn(Protocol\BasicReturnFrame $frame): void
    {
        if ($this->state !== self::STATE_WAIT) {
            return;
        }

        $this->return = $frame;
        $this->state = self::STATE_HEAD;
    }

    public function receiveDeliver(Protocol\BasicDeliverFrame $frame): void
    {
        if ($this->state !== self::STATE_WAIT) {
            return;
        }

        $this->deliver = $frame;
        $this->state = self::STATE_HEAD;
    }

    public function receiveHeader(Protocol\ContentHeaderFrame $frame): void
    {
        if ($this->state !== self::STATE_HEAD) {
            return;
        }

        $this->state = self::STATE_BODY;
        $this->header = $frame;
        $this->remaining = $frame->bodySize;

        $this->runCallbacks();
    }

    public function receiveBody(Protocol\ContentBodyFrame $frame): void
    {
        if ($this->state !== self::STATE_BODY) {
            return;
        }

        $this->buffer->append((string)$frame->payload);

        $this->remaining -= (int)$frame->size;

        if ($this->remaining < 0) {
            throw Exception\ChannelException::bodyOverflow($this->remaining);
        }

        $this->runCallbacks();
    }

    /**
     * @throws \PHPinnacle\Ridge\Exception\ChannelException
     */
    private function runCallbacks(): void
    {
        if ($this->remaining !== 0) {
            return;
        }

        if ($this->return) {
            $message = new Message(
                content: $this->buffer->flush(),
                exchange: $this->return->exchange,
                routingKey: $this->return->routingKey,
                consumerTag: null,
                deliveryTag: null,
                redelivered: false,
                returned: true,
                headers: $this->header !== null ? $this->header->toArray() : []
            );
        } else {
            if ($this->deliver) {
                $message = new Message(
                    content: $this->buffer->flush(),
                    exchange: $this->deliver->exchange,
                    routingKey: $this->deliver->routingKey,
                    consumerTag: $this->deliver->consumerTag,
                    deliveryTag: $this->deliver->deliveryTag,
                    redelivered: $this->deliver->redelivered,
                    returned: false,
                    headers: $this->header !== null ? $this->header->toArray() : []
                );
            } else {
                throw Exception\ChannelException::frameOrder();
            }
        }

        $this->return = null;
        $this->deliver = null;
        $this->header = null;

        foreach ($this->callbacks as $callback) {
            /** @psalm-suppress MixedArgumentTypeCoercion */
            async($callback, $message);
        }

        $this->state = self::STATE_WAIT;
    }
}
