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

use Amp\DeferredFuture;

final class CommandWaitQueue
{
    /** @var DeferredFuture[] */
    private array $waitingCommands = [];

    public function add(DeferredFuture $deferred): void {
        $this->waitingCommands[spl_object_hash($deferred)] = $deferred;
        $deferred->getFuture()->finally(function() use ($deferred) {
            unset($this->waitingCommands[spl_object_hash($deferred)]);
        });
    }

    public function cancel(\Throwable $throwable): void {
        foreach ($this->waitingCommands as $id => $deferred) {
            $deferred->error($throwable);
            unset($this->waitingCommands[$id]);
        }
    }
}
