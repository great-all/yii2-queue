<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace great\queue;

use Yii;
use yii\base\Component;
use yii\base\InvalidConfigException;
use yii\helpers\Json;

/**
 * RedisQueue
 *
 * @author Alexander Kochetov <creocoder@gmail.com>
 */
class RedisQueue extends Component implements QueueInterface
{
    /**
     * @var Client|array
     */
    public $redis;
    /**
     * @var integer
     */
    public $expire = 60;

    /**
     * @inheritdoc
     */
    public function init()
    {
        parent::init();

        if(is_string($this->redis)) {
            $this->redis = \yii::$app->get($this->redis);
        }

        if(is_array($this->redis)) {
            $this->redis = yii::createObject($this->redis);
        }

        if ($this->redis === null) {
            throw new InvalidConfigException('The "redis" property must be set.');
        }
    }

    /**
     * @inheritdoc
     */
    public function push($payload, $queue, $delay = 0)
    {
        $payload = Json::encode(['id' => $id = md5(uniqid('', true)), 'body' => $payload]);

        if ($delay > 0) {
            $this->redis->executeCommand('ZADD', [$queue . ':delayed', time() + $delay,$payload ]);
        } else {
            $this->redis->executeCommand('RPUSH',[$queue,$payload]);
        }

        return $id;
    }

    /**
     * @inheritdoc
     */
    public function pop($queue)
    {
        foreach ([':delayed', ':reserved'] as $type) {
            $data = $this->redis->executeCommand('ZRANGEBYSCORE',[$queue . $type, '-inf', $time = time()]);
            if (!empty($data)) {
                //开启事务
                //$this->redis->executeCommand('WATCH',[$queue . $type]);//暂时不用监听队列
                $this->redis->executeCommand('MULTI');

                $this->redis->executeCommand('ZREMRANGEBYSCORE',[$queue . $type, '-inf', $time]);
                $this->redis->executeCommand('RPUSH', array_merge([$queue],$data));

                //执行事务中的命令
                $this->redis->executeCommand('EXEC');
            }
        }

        $data = $this->redis->executeCommand('LPOP',[$queue]);

        if ($data === null) {
            return false;
        }

        //$this->redis->zadd($queue . ':reserved', [$data => time() + $this->expire]);
        $data = Json::decode($data);

        return [
            'id' => $data['id'],
            'body' => $data['body'],
            'queue' => $queue,
        ];
    }

    /**
     * @inheritdoc
     */
    public function purge($queue) {
        $this->redis->executeCommand('DEL',[$queue.':delayed',$queue.':reserved',$queue]);
    }

    /**
     * @inheritdoc
     */
    public function release(array $message, $delay = 0)
    {
        if ($delay > 0) {
            $this->redis->executeCommand('ZADD',[$message['queue'] . ':delayed',time() + $delay,$message['body']]);
        } else {
            $this->redis->executeCommand('RPUSH',[$message['queue'],$message['body']]);
        }
    }

    /**
     * @inheritdoc
     */
    public function delete(array $message)
    {
        $this->redis->executeCommand('ZREM',[$message['queue'] . ':reserved',$message['body']]);
    }
}
