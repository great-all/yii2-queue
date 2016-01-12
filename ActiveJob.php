<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace great\queue;

use Yii;
use yii\base\Object;

/**
 * ActiveJob
 *
 * @author Alexander Kochetov <creocoder@gmail.com>
 */
abstract class ActiveJob extends Object
{
    /**
     * @var array
     */
    public $serializer = ['serialize', 'unserialize'];

    /**
     * Runs the job.
     */
    abstract public function run();

    /**
     * @return string
     */
    abstract public function queueName();

    /**
     * @return QueueInterface
     */
    public static function getQueue()
    {
        return Yii::$app->get('queue');
    }

    /**
     * Pushs the job.
     *
     * @param integer $delay
     * @return string
     */
    public function push($delay = 0)
    {
        return $this->getQueue()->push([
            'serializer' => $this->serializer,
            'object' => call_user_func($this->serializer[0], $this),
        ], $this->queueName(), $delay);
    }
}
