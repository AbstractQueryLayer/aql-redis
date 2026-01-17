<?php

declare(strict_types=1);

namespace IfCastle\AQL\Redis\Storage;

use IfCastle\AQL\Dsl\BasicQueryInterface;
use IfCastle\AQL\Dsl\Sql\Helpers\ConditionsHelper;
use IfCastle\AQL\Dsl\Sql\Helpers\JoinHelper;
use IfCastle\AQL\Dsl\Sql\Helpers\TupleHelper;
use IfCastle\AQL\Dsl\Sql\Query\QueryInterface;
use IfCastle\AQL\Entity\EntityInterface;
use IfCastle\AQL\Executor\Exceptions\QueryException;
use IfCastle\AQL\Executor\QueryExecutorInterface;
use IfCastle\AQL\Executor\QueryExecutorResolverInterface;
use IfCastle\AQL\Result\ResultInterface;
use IfCastle\AQL\Storage\AqlStorageInterface;
use IfCastle\AQL\Storage\Exceptions\StorageException;
use IfCastle\AQL\Transaction\TransactionInterface;

class Redis implements AqlStorageInterface, QueryExecutorResolverInterface
{
    final public const string REDIS        = 'redis';

    final public const string STRUCTURE_TYPE = 'type';

    protected ?string $storageName  = self::REDIS;

    protected \Redis  $redis;

    protected ?TransactionInterface $transaction = null;

    protected ?StorageException $lastError = null;

    #[\Override]
    public function getStorageName(): ?string
    {
        return $this->storageName;
    }

    #[\Override]
    public function setStorageName(string $storageName): static
    {
        $this->storageName          = $storageName;
        return $this;
    }

    #[\Override]
    public function connect(): void {}

    #[\Override]
    public function getLastError(): ?StorageException
    {
        return $this->lastError;
    }

    #[\Override]
    public function disconnect(): void {}

    #[\Override] public function resolveQueryExecutor(
        BasicQueryInterface $basicQuery,
        ?EntityInterface     $entity = null
    ): ?QueryExecutorInterface {
        return new RedisQueryExecutor();
    }

    /**
     * @throws StorageException
     */
    #[\Override]
    public function executeAql(BasicQueryInterface $query, ?object $context = null): ResultInterface
    {
        if ($query instanceof QueryInterface === false) {
            throw new StorageException('Query must be instance of SqlQueryI');
        }

        return match ($query->getResolvedAction()) {
            QueryInterface::ACTION_SELECT                                 => $this->executeSelect($query->resolveSubstitution(), $context),
            QueryInterface::ACTION_INSERT, QueryInterface::ACTION_REPLACE => $this->executeInsert($query->resolveSubstitution(), $context),
            QueryInterface::ACTION_UPDATE                                 => $this->executeUpdate($query->resolveSubstitution(), $context),
            QueryInterface::ACTION_DELETE                                 => $this->executeDelete($query->resolveSubstitution(), $context),
            default                                                       => throw new StorageException([
                'template' => 'Unknown query action: {action}',
                'action' => $query->getResolvedAction(),
                'storage' => $this->getStorageName(),
                'query' => $query->getAql(),
            ]),
        };
    }

    /**
     * @throws QueryException
     * @throws StorageException
     */
    protected function executeSelect(QueryInterface $query, ?object $context = null): ResultInterface
    {
        if ($query->getLimit()?->isNotEmpty()) {

        }

        $subjects                   = JoinHelper::toPlainAliasJoinList($query);

        if ($subjects === []) {
            throw new StorageException([
                'template'          => 'No subjects found in query',
                'storage'           => $query->getQueryStorage(),
                'query'             => $query->getAql(),
            ]);
        }

        $mainSubjectAlias           = \array_keys($subjects)[0];

        TupleHelper::groupTupleColumnsBySubject($query);
        $filtersBySubject           = ConditionsHelper::groupFiltersBySubject($query);

        //
        // For a Redis database, we are trying to select data in JOIN order in the query.
        // We do not optimize the query and do not try to guess which order will lead to a more optimal selection.
        // Please note that Redis storage does not support Relation with complex conditions.
        //
        //

        $mainEntity                 = $context->getMainEntity();
        $structureType              = RedisStructureTypes::tryFrom($mainEntity->getOptions()->find(self::REDIS, self::STRUCTURE_TYPE))
            ?? RedisStructureTypes::HASH;


        if ($filtersBySubject === []) {

            // getAll algorithm


        }

        $mainEntity                 = $context->getMainEntity();
        $structureType              = RedisStructureTypes::tryFrom($mainEntity->getOptions()->find(self::REDIS, self::STRUCTURE_TYPE))
                                    ?? RedisStructureTypes::HASH;

        if ($structureType !== RedisStructureTypes::HASH && \count($filtersBySubject[$mainSubjectAlias]) > 1) {
            throw new StorageException([
                'template'          => 'Multiple Filters are not supported for Redis {structureType} structure type',
                'storage'           => $query->getQueryStorage(),
                'query'             => $query->getAql(),
                'structureType'     => $structureType->value,
            ]);
        }


    }

    protected function executeInsert(QueryInterface $query, ?object $context = null): ResultInterface
    {
        return new RedisResult();
    }

    protected function executeUpdate(QueryInterface $query, ?object $context = null): ResultInterface
    {
        return new RedisResult();
    }

    protected function executeDelete(QueryInterface $query, ?object $context = null): ResultInterface
    {
        return new RedisResult();
    }

    /**
     * @throws StorageException
     */
    protected function getAllRecords(EntityInterface $entity, string $subject): array
    {
        $structureType              = RedisStructureTypes::tryFrom($entity->getOptions()->find(self::REDIS, self::STRUCTURE_TYPE))
            ?? RedisStructureTypes::HASH;

        return match ($structureType) {
            RedisStructureTypes::LIST       => $this->scanList($subject . ':*'),
            RedisStructureTypes::SET        => $this->scanSet($subject . ':*'),
            RedisStructureTypes::HASH       => $this->scanHash($subject . ':*'),
            RedisStructureTypes::SORTED_SET => $this->scanSortedSet($subject . ':*'),
            default                         => throw new StorageException([
                'template'                  => 'Unknown Redis structure type: {structureType}',
                'storage'                   => $this->getStorageName(),
                'structureType'             => $structureType->value,
            ])
        };
    }

    protected function scanList(?string $pattern = null): array
    {
        $results                    = [];
        $cursor                     = '0';

        do {
            $result                 = $this->redis->scan($cursor, 'MATCH', $pattern, 'COUNT', 100);

            foreach ($result[1] as $key) {
                $results[]          = $this->redis->get($key);
            }

            $cursor                 = $result[0];
        } while ($cursor !== '0');

        return $results;
    }

    protected function scanSet(string $setKey, ?string $pattern = null): array
    {
        $results                    = [];
        $cursor                     = '0';

        do {
            $result                 = $this->redis->sScan($setKey, $cursor, 'MATCH', $pattern, 'COUNT', 100);

            foreach ($result as $member) {
                $results[] = $member;
            }

            $cursor = $result[0];
        } while ($cursor !== '0');

        return $results;
    }

    protected function scanHash(string $hashKey, ?string $pattern = null): array
    {
        $results                    = [];
        $cursor                     = '0';

        do {
            $result                 = $this->redis->hScan($hashKey, $cursor, 'MATCH', $pattern, 'COUNT', 100);

            foreach ($result as $key => $value) {
                $results[$key]      = $value;
            }

            $cursor                 = $result[0];
        } while ($cursor !== '0');

        return $results;
    }

    protected function scanSortedSet(string $sortedSetKey, ?string $pattern = null): array
    {
        $results                    = [];
        $cursor                     = '0';

        do {
            $result                 = $this->redis->zScan($sortedSetKey, $cursor, 'MATCH', $pattern, 'COUNT', 100);

            foreach ($result as $key => $value) {
                $results[$key]      = $value;
            }

            $cursor                 = $result[0];
        } while ($cursor !== '0');

        return $results;
    }
}
