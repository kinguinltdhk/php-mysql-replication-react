<?php

namespace MySQLReplicationReact;

use Doctrine\DBAL\DBALException;
use MySQLReplication\BinLog\Exception\BinLogException;
use MySQLReplication\Config\Config;
use MySQLReplication\Config\Exception\ConfigException;
use MySQLReplication\Exception\MySQLReplicationException;
use MySQLReplication\MySQLReplicationFactory;
use React\EventLoop\Factory;
use React\EventLoop\LoopInterface;

/**
 * Class MySQLReplicationFactory
 * @package MySQLReplication
 */
class MySQLReplicationReactFactory extends MySQLReplicationFactory
{
    /**
     * @var LoopInterface
     */
    private $loop;
    
    /**
     * @param Config $config
     * @throws MySQLReplicationException
     * @throws DBALException
     * @throws ConfigException
     * @throws BinLogException
     */
    public function __construct(Config $config, LoopInterface $loop = null)
    {
        $this->setLoop($loop);
        parent::__construct($config);
    }

    /**
     * @param Config $config
     * @return BinLogConnectReact
     */
    protected function getBinLogConnect(Config $config)
    {
        return new BinLogConnectReact($config, $this->getMySQLRepository(), $this->getBinLogAuth(), $this->getGtiService(), $this->loop);
    }

    private function setLoop(LoopInterface $loop)
    {
        if (is_null($loop)) {
            $loop = Factory::create();
        }
        $this->loop = $loop;
    }

    /**
     * @return LoopInterface
     */
    public function getLoop()
    {
        return $this->loop;
    }
}
