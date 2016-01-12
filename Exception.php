<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace great\queue;

/**
 * Exception
 *
 * @author Alexander Kochetov <creocoder@gmail.com>
 */
class Exception extends \yii\base\Exception
{
    /**
     * @inheritdoc
     */
    public function getName()
    {
        return 'Queue Exception';
    }
}
