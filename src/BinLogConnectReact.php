<?php

namespace MySQLReplicationReact;

use MySQLReplication\BinaryDataReader\BinaryDataReader;
use MySQLReplication\BinLog\BinLogAuth;
use MySQLReplication\BinLog\BinLogConnectInterface;
use MySQLReplication\BinLog\BinLogServerInfo;
use MySQLReplication\BinLog\Exception\BinLogException;
use MySQLReplication\Config\Config;
use MySQLReplication\Definitions\ConstCapabilityFlags;
use MySQLReplication\Definitions\ConstCommand;
use MySQLReplication\Gtid\GtidService;
use MySQLReplication\MySQLReplicationFactory;
use MySQLReplication\Repository\MySQLRepository;
use React\EventLoop\LoopInterface;
use React\Promise\Deferred;
use React\Promise\Promise;
use React\SocketClient\TcpConnector;
use React\Stream\Stream;

/**
 * Class BinLogConnectReact
 * @package MySQLReplication\BinLog
 */
class BinLogConnectReact implements BinLogConnectInterface
{
    /**
     * @var Stream
     */
    private $socket;
    /**
     * @var bool
     */
    private $checkSum = false;
    /**
     * @var MySQLRepository
     */
    private $mySQLRepository;
    /**
     * @var Config
     */
    private $config;
    /**
     * @var BinLogAuth
     */
    private $packAuth;
    /**
     * http://dev.mysql.com/doc/internals/en/auth-phase-fast-path.html 00 FE
     * @var array
     */
    private $packageOkHeader = [0, 254];

    /**
     * @var LoopInterface
     */
    private $loop;

    /**
     * @var string
     */
    private $buffer;

    /**
     * @param Config $config
     * @param MySQLRepository $mySQLRepository
     * @param BinLogAuth $packAuth
     * @param GtidService $gtidService
     * @param LoopInterface $loop
     */
    public function __construct(
        Config $config,
        MySQLRepository $mySQLRepository,
        BinLogAuth $packAuth,
        GtidService $gtidService,
        LoopInterface $loop
    ) {
        $this->mySQLRepository = $mySQLRepository;
        $this->config = $config;
        $this->packAuth = $packAuth;
        $this->gtidService = $gtidService;
        $this->loop = $loop;
    }

    public function __destruct()
    {
        if (true === $this->isConnected())
        {
            socket_shutdown($this->socket);
            socket_close($this->socket);
        }
    }

    /**
     * @return bool
     */
    public function isConnected()
    {
        return is_resource($this->socket);
    }

    /**
     * @return bool
     */
    public function getCheckSum()
    {
        return $this->checkSum;
    }

    /**
     * @throws BinLogException
     */
    public function connectToStream(MySQLReplicationFactory $factory)
    {
        if (false === filter_var($this->config->getIp(), FILTER_VALIDATE_IP)) {
            $ip = gethostbyname($this->config->getIp());
            if ($ip == $this->config->getIp()) {
                throw new BinLogException('Unable to resolve hostname: ' . $this->config->getIp());
            }
        } else {
            $ip = $this->config->getIp();
        }
        $s = new TcpConnector($this->loop);
        return $s->create($ip, $this->config->getPort())->then(function(Stream $st) use ($factory) {
            $this->socket = $st;
            $this->socket->bufferSize = null;
            if (!$this->socket->isReadable() || !$this->socket->isWritable()) {
                throw new BinLogException(socket_strerror(socket_last_error()), socket_last_error());
            }

            $this->socket->on('data', function ($data) {
                $this->buffer = $this->buffer . $data;
            });

            $this->socket->on('error', function () {
                throw new BinLogException('BinLog socket error');
            });
            $this->socket->on('disconnect', function () {
                throw new BinLogException('BinLog socket disconnect');
            });
            $this->socket->on('end', function () {
                throw new BinLogException('BinLog socket end');
            });
            $this->socket->on('close', function () {
                throw new BinLogException('BinLog socket close');
            });

            return $this->serverInfo()->then(function() {
                return $this->auth();
            })->then(function() {
                return $this->getBinlogStream();
            })->then(function() use ($factory) {
                $this->socket->on('binlog', function() use ($factory) {
                    $factory->binLogEvent();
                });
                $this->socket->on('data', function ($data) {
                    while (!empty($this->buffer)) {
                        try {
                            $before = $this->buffer;
                            $this->socket->emit('binlog');
                        } catch (\Exception $e) {
                            $this->buffer = $before;
                            break;
                        }
                    }
                });
            });
        }, function() {
            throw new BinLogException('Unable to create a socket:' . socket_strerror(socket_last_error()), socket_last_error());
        });

    }

    /**
     * @return Promise|\React\Promise\PromiseInterface
     */
    private function getPacketPromise()
    {
        $d = new Deferred();
        $this->socket->once('data', function() use ($d) {
            try {
                $packet = $this->getPacket();
                if (!$packet) {
                    return $d->reject();
                }
                return $d->resolve();
            } catch (\Exception $e) {
                return $d->reject($e);
            }
        });
        return $d->promise();
    }

    /**
     * @throws BinLogException
     */
    private function serverInfo()
    {
        $d = new Deferred();
        $this->socket->once('data', function() use ($d) {
            $packet = $this->getPacket(false);
            if (!$packet) {
                return $d->reject();
            }
            return $d->resolve(BinLogServerInfo::parsePackage($packet));
        });
        return $d->promise();
    }

    /**
     * @param bool $checkForOkByte
     * @return string
     * @throws BinLogException
     */
    public function getPacket($checkForOkByte = true)
    {
        $header = $this->readFromSocket(4);
        if (false === $header)
        {
            return false;
        }
        $dataLength = unpack('L', $header[0] . $header[1] . $header[2] . chr(0))[1];

        $result = $this->readFromSocket($dataLength);
        if (true === $checkForOkByte)
        {
            $this->isWriteSuccessful($result);
        }

        return $result;
    }

    /**
     * @param $length
     * @return mixed
     * @throws BinLogException
     */
    private function readFromSocket($length)
    {
        $buf = substr($this->buffer, 0, $length);
        $received = strlen($buf);
        $this->buffer = substr($this->buffer, $received);
        if ($length === $received)
        {
            return $buf;
        }

        // http://php.net/manual/pl/function.socket-recv.php#47182
        if (0 === $received)
        {
            throw new BinLogException('Disconnected by remote side');
        }

        throw new BinLogException(socket_strerror(socket_last_error()), socket_last_error());
    }

    /**
     * @param $packet
     * @return array
     * @throws BinLogException
     */
    public function isWriteSuccessful($packet)
    {
        $head = ord($packet[0]);
        if (in_array($head, $this->packageOkHeader, true))
        {
            return ['status' => true, 'code' => 0, 'msg' => ''];
        }
        else
        {
            $error_code = unpack('v', $packet[1] . $packet[2])[1];
            $error_msg = '';
            $packetLength = strlen($packet);
            for ($i = 9; $i < $packetLength; $i++)
            {
                $error_msg .= $packet[$i];
            }

            throw new BinLogException($error_msg, $error_code);
        }
    }

    /**
     * @throws BinLogException
     */
    private function auth()
    {
        $data = $this->packAuth->createAuthenticationBinary(
            ConstCapabilityFlags::getCapabilities(),
            $this->config->getUser(),
            $this->config->getPassword(),
            BinLogServerInfo::getSalt()
        );

        $this->writeToSocket($data);
        return $this->getPacketPromise();
    }

    /**
     * @param $data
     * @throws BinLogException
     */
    private function writeToSocket($data)
    {
        $this->socket->write($data);
//        if (false === socket_write($this->socket, $data, strlen($data)))
//        {
//            throw new BinLogException('Unable to write to socket: ' . socket_strerror(socket_last_error()), socket_last_error());
//        }
    }

    /**
     * @throws BinLogException
     */
    private function getBinlogStream()
    {
        $this->checkSum = $this->mySQLRepository->isCheckSum();
        $p = new Promise(function(){return;});
        if (true === $this->checkSum)
        {
            $p = $this->execute('SET @master_binlog_checksum=@@global.binlog_checksum');
        }

        return $p->then(function() {
            return $this->registerSlave();
        })->then(function() {
            if ('' !== $this->config->getGtid())
            {
                return $this->setBinLogDumpGtid();
            }
            else
            {
                return $this->setBinLogDump();
            }
        });

        
    }

    /**
     * @param string $sql
     * @throws BinLogException
     */
    private function execute($sql)
    {
        $chunk_size = strlen($sql) + 1;
        $prelude = pack('LC', $chunk_size, 0x03);

        $this->writeToSocket($prelude . $sql);
        return $this->getPacketPromise();
    }

    /**
     * @see https://dev.mysql.com/doc/internals/en/com-register-slave.html
     * @throws BinLogException
     */
    private function registerSlave()
    {
        $prelude = pack('l', 18) . chr(ConstCommand::COM_REGISTER_SLAVE);
        $prelude .= pack('I', $this->config->getSlaveId());
        $prelude .= chr(0);
        $prelude .= chr(0);
        $prelude .= chr(0);
        $prelude .= pack('s', '');
        $prelude .= pack('I', 0);
        $prelude .= pack('I', 0);

        $this->writeToSocket($prelude);
        return $this->getPacketPromise();
    }

    /**
     * @see https://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html
     * @throws BinLogException
     */
    private function setBinLogDumpGtid()
    {
        $collection = $this->gtidService->makeCollectionFromString($this->config->getGtid());

        $prelude = pack('l', 26 + $collection->getEncodedLength()) . chr(ConstCommand::COM_BINLOG_DUMP_GTID);
        $prelude .= pack('S', 0);
        $prelude .= pack('I', $this->config->getSlaveId());
        $prelude .= pack('I', 3);
        $prelude .= chr(0);
        $prelude .= chr(0);
        $prelude .= chr(0);
        $prelude .= BinaryDataReader::pack64bit(4);

        $prelude .= pack('I', $collection->getEncodedLength());
        $prelude .= $collection->getEncoded();

        $this->writeToSocket($prelude);
        return $this->getPacketPromise();
    }

    /**
     * @see https://dev.mysql.com/doc/internals/en/com-binlog-dump.html
     * @throws BinLogException
     */
    private function setBinLogDump()
    {

        $p = new Promise(function(){return;});

        if ('' !== $this->config->getMariaDbGtid())
        {
            $p = $this->execute('SET @mariadb_slave_capability = 4')->then(function(){
                return $this->execute('SET @slave_connect_state = \'' . $this->config->getMariaDbGtid() . '\'');
            })->then(function() {
                return $this->execute('SET @slave_gtid_strict_mode = 0');
            })->then(function() {
                return $this->execute('SET @slave_gtid_ignore_duplicates = 0');
            });

        }

        return $p->then(function() {
            $binFilePos = $this->config->getBinLogPosition();
            $binFileName = $this->config->getBinLogFileName();

            if ('' === $binFilePos || '' === $binFileName)
            {
                $master = $this->mySQLRepository->getMasterStatus();
                $binFilePos = $master['Position'];
                $binFileName = $master['File'];
            }

            $prelude = pack('i', strlen($binFileName) + 11) . chr(ConstCommand::COM_BINLOG_DUMP);
            $prelude .= pack('I', $binFilePos);
            $prelude .= pack('v', 0);
            $prelude .= pack('I', $this->config->getSlaveId());
            $prelude .= $binFileName;

            $this->writeToSocket($prelude);
            return $this->getPacketPromise();
        });

    }
}
