<?php

declare(strict_types=1);

namespace IfCastle\AQL\Redis\Storage;

enum RedisStructureTypes: string
{
    case LIST                       = 'list';
    case SET                        = 'set';
    case HASH                       = 'hash';
    case SORTED_SET                 = 'zset';
}
