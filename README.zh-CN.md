# Yii2 RabbitMQ 扩展

[English](README.md) · 简体中文

本扩展基于 php-amqplib，让 Yii2 应用可以通过 RabbitMQ 发布和消费消息。设计参考了 Symfony 的 RabbitMqBundle。

`xincheng/yii2-rabbitmq` 是独立维护的分支。[上游项目 mikemadisonweb/yii2-rabbitmq](https://github.com/mikemadisonweb/yii2-rabbitmq)

当前分支支持 PHP 7.4 和 PHP 8.x（包括 PHP 8.5），依赖 Yii 2.x（最低 2.0.55）和 php-amqplib 3.x（最低 3.7.4）。仍在使用 PHP 5.4 等旧版本的项目可查阅[旧版扩展说明](https://github.com/mikemadisonweb/yii2-rabbitmq/blob/master/README_v1.md)。


快速定位：[在 Yii 中配置 SAC](#在-yii-中配置-sac) · [迁移已有队列](#迁移已有队列) · [长任务的心跳与确认超时](#长任务的心跳与确认超时) · [配置项](#配置项)

## 安装

在项目目录下用 Composer 安装扩展。

```sh
composer require xincheng/yii2-rabbitmq
```

从上游包迁移时，先从应用的 Composer 依赖中移除 `mikemadisonweb/yii2-rabbitmq`，再安装本分支。两个包使用相同的 PHP 命名空间 `mikemadisonweb\rabbitmq`，原有的 `use` 声明和 Yii 配置中的类名可以继续使用。

## 配置

在 Web 和控制台应用共用的配置里注册 `rabbitmq` 组件。下面的示例使用默认虚拟主机（vhost）`/`，通过直连交换器将消息路由到队列，并按队列名关联消费回调。[RabbitMQ 发布与订阅教程](https://www.rabbitmq.com/tutorials/tutorial-three-php.html)

```php
<?php
return [
    // 放在 Web 和控制台应用共用的配置中，例如 common.php。
    'components'    => [
        // ...
        'rabbitmq' => [
            'class' => \mikemadisonweb\rabbitmq\Configuration::class,
            'connections' => [
                [
                    // 也可以用一个 url 表示这些连接参数，见 https://www.rabbitmq.com/uri-spec.html
                    'host' => 'YOUR_HOSTNAME',
                    'port' => '5672',
                    'user' => 'YOUR_USERNAME',
                    'password' => 'YOUR_PASSWORD',
                    'vhost' => '/',
                ]
                // 配置多个连接时，分别设置 name，并在生产者和消费者配置中通过 connection 引用。
            ],
            'exchanges' => [
                [
                    'name' => 'YOUR_EXCHANGE_NAME',
                    'type' => 'direct'
                    // 其他选项请参见配置参考。
                ],
            ],
            'queues' => [
                [
                    'name' => 'YOUR_QUEUE_NAME',
                    // 在此设置队列属性。
                    //'durable' => true,
                    //'auto_delete' => false,
                ],
                [
                    'name' => 'YOUR_ANOTHER_QUEUE_NAME',
                ],
            ],
            'bindings' => [
                [
                    'queue' => 'YOUR_QUEUE_NAME',
                    'exchange' => 'YOUR_EXCHANGE_NAME',
                    'routing_keys' => ['YOUR_ROUTING_KEY'],
                ],
            ],
            'producers' => [
                [
                    'name' => 'YOUR_PRODUCER_NAME',
                ],
            ],
            'consumers' => [
                [
                    'name' => 'YOUR_CONSUMER_NAME',
                    // 为消费者订阅的每个队列指定一个回调。
                    'callbacks' => [
                        // 队列名 => 回调类名
                        'YOUR_QUEUE_NAME' => \path\to\YourConsumer::class,
                    ],
                ],
            ],
        ],
    ],
];
```

如果刚接触 RabbitMQ，可以先了解交换器、队列和绑定之间的关系。[RabbitMQ 基础概念](https://mikemadisonweb.github.io/2017/05/04/tldr-series-rabbitmq/)

`callbacks` 的值可以是回调类名，也可以是 Yii 依赖注入容器中的服务名。例如，下面将消费回调注册为单例，在 `callbacks` 中可用 `'YOUR_QUEUE_NAME' => 'rabbitmq.import-data.consumer'` 引用它。Yii 自 2.0.11 起支持在应用配置中定义容器服务。[Yii 依赖注入容器](http://www.yiiframework.com/doc-2.0/yii-di-container.html)

```php
<?php
use yii\di\Instance;

return [
    // ...
    'container' => [
        'definitions' => [],
        'singletons' => [
            'rabbitmq.import-data.consumer' => [
                [
                    'class' => \path\to\YourConsumer::class,
                ],
                [
                    // 如需注入依赖，键名应与构造函数的参数名相同。
                    'someDependency' => Instance::of('dependency-service-name'),
                ],
            ],
        ],
    ],
];
```

每个命名消费者可以单独启动一个常驻工作进程（worker）。如果多个队列的任务较轻，希望用一个进程处理，可以在同一个消费者的 `callbacks` 中为各个队列指定回调。

配置绑定时，请核对交换器名、队列名和路由键，让消息到达预期的队列。

### 生命周期事件

扩展提供 `before_consume`、`after_consume`、`before_publish`、`after_publish` 四个事件，可分别在消息消费或发布前后执行额外操作。例如，常驻消费者的数据库连接可能因空闲超时而失效。下面在处理消息前检查已打开的连接。如果查询失败，就关闭连接，让 Yii 在下次访问数据库时重新连接。

```php
<?php
// config/main.php
return [
    // ...
    'components'    => [
        // ...
        'rabbitmq'  => [
            // ...
            'on before_consume' => function ($event) {
                if (\Yii::$app->has('db') && \Yii::$app->db->isActive) {
                    try {
                        \Yii::$app->db->createCommand('SELECT 1')->query();
                    } catch (\yii\db\Exception $exception) {
                        \Yii::$app->db->close();
                    }
                }
            },
        ],
        // ...
    ],
];
```

### 日志

`logger` 为可选配置。下面的示例将消息处理记录写入 Yii 应用日志，并关闭控制台输出。

```php
<?php
// config/main.php
return [
    // ...
    'components'    => [
        // ...
        'rabbitmq'  => [
            // ...
            'logger' => [
                'log' => true,
                'category' => 'application',
                'print_console' => false,
                'system_memory' => false,
            ],
        ],
        // ...
    ],
];
```

默认情况下，扩展关闭 Yii 日志记录。设置 `log` 为 `true` 后，记录会写入应用日志，也可以通过 `category` 将它们送到相应的日志目标。调试消费者时，可用 `print_console` 控制终端是否显示处理结果。

### 示例项目

可以使用基于 Yii2 Basic 模板的示例项目，在独立环境中运行扩展并调试自己的配置。[Yii2 RabbitMQ 示例项目](https://bitbucket.org/MikeMadison/yii2-rabbitmq-test)

## 控制台命令

| 命令 | 用途 |
| --- | --- |
| `rabbitmq/consume` | 启动指定的消费者。 |
| `rabbitmq/restart-consume` | 在当前进程中用新连接启动指定的消费者。要重新加载由 systemd 管理的现有消费者，请向其主进程发送 `SIGHUP`。 |
| `rabbitmq/declare-all` | 按配置声明交换器、队列及绑定。 |
| `rabbitmq/declare-exchange` | 声明配置中指定的交换器。 |
| `rabbitmq/declare-queue` | 声明配置中指定的队列。 |
| `rabbitmq/delete-all` | 删除指定连接上配置的所有交换器和队列。 |
| `rabbitmq/delete-exchange` | 删除指定的交换器。 |
| `rabbitmq/delete-queue` | 删除指定的队列。 |
| `rabbitmq/publish` | 从标准输入读取消息并发布到指定交换器，由交换器路由到队列。 |
| `rabbitmq/purge-queue` | 清空指定队列中等待投递的消息。 |

使用配置中的消费者名称启动进程。

```
yii rabbitmq/consume YOUR_CONSUMER_NAME
```

可以使用 Supervisor 等进程管理器在消费者退出后重新启动它，使其持续运行。

### 限制内存和处理条数

常驻 PHP 进程可以按内存用量或已处理消息数退出，再由进程管理器重新启动。`--memoryLimit` 的单位是 MiB，`--messagesLimit` 的单位是条。两者都默认为 `0`，表示不设该项限制。

```
--memoryLimit, -l  （默认为 0）
--messagesLimit, -m  （默认为 0）
```

### 自动声明

扩展默认启用 `auto_declare`。消费者启动或生产者发布消息时，扩展会按当前连接的配置声明所需的交换器、队列和绑定。同一连接会话会复用已声明的拓扑。若要在部署阶段统一管理这些资源，可设置 `'auto_declare' => false`，然后用控制台命令创建和删除它们。

若某个队列或交换器由其他应用管理，可在该资源上设置 `'declare' => false`。自动声明和 `rabbitmq/declare-all` 会跳过该资源，配置的绑定仍可使用它。需要手动创建时，可以显式执行 `declare-queue` 或 `declare-exchange`。

### 多个虚拟主机（vhost）

为每个连接指定 `name`，并在生产者和消费者的 `connection` 中引用这个名称。要把某个队列、交换器或绑定限定在一个虚拟主机上，也要为它设置相同的 `connection`。

```php
'connections' => [
    ['name' => 'a', 'host' => 'localhost', 'vhost' => '/a'],
    ['name' => 'b', 'host' => 'localhost', 'vhost' => '/b'],
],
'exchanges' => [
    ['name' => 'events', 'type' => 'direct'],
],
'queues' => [
    ['name' => 'orders', 'connection' => 'a'],
    ['name' => 'billing', 'connection' => 'b'],
],
'bindings' => [
    ['exchange' => 'events', 'queue' => 'orders', 'routing_keys' => ['order']],
    ['exchange' => 'events', 'queue' => 'billing', 'routing_keys' => ['invoice']],
],
```

运行 `yii rabbitmq/declare-all a` 会在虚拟主机 `/a` 上创建 `events` 交换器、`orders` 队列和相应的绑定。运行 `yii rabbitmq/declare-all b` 则会在 `/b` 上创建 `events`、`billing` 和相应的绑定。省略 `connection` 的队列和交换器对每个选中的连接都生效。绑定仅在两端资源均属于当前连接时生效。不同连接可以使用同名队列或交换器，并分别设置参数。

### 在指定虚拟主机上使用单活消费者（SAC）

#### 在 Yii 中配置 SAC

单活消费者（Single Active Consumer，简称 SAC）是 RabbitMQ 内置的队列功能，适用于经典队列（classic queue）和仲裁队列（quorum queue）。在 Yii 控制台应用加载的配置中，找到目标队列的 `components.rabbitmq.queues[].arguments`，加入 `'x-single-active-consumer' => ['t', true]`。其中 `'t'` 代表 AMQP 布尔类型。RabbitMQ 在首次创建队列时应用这个参数。[RabbitMQ 单活消费者文档](https://www.rabbitmq.com/docs/consumers#single-active-consumer)

下面把队列和消费者的 `connection` 都设为 `a`，指向虚拟主机 `/a`。请换成实际的连接凭据，并让 `JobsConsumer` 实现 `ConsumerInterface`。若已配置连接 `a`，只需将新的队列和消费者条目分别追加到已有列表。

```php
<?php
return [
    'components' => [
        'rabbitmq' => [
            'class' => \mikemadisonweb\rabbitmq\Configuration::class,
            'connections' => [[
                'name' => 'a', 'host' => 'localhost', 'vhost' => '/a',
                'user' => 'YOUR_USERNAME', 'password' => 'YOUR_PASSWORD',
            ]],
            'queues' => [[
                'name' => 'jobs.sac',
                'connection' => 'a',
                'arguments' => ['x-single-active-consumer' => ['t', true]],
            ]],
            'consumers' => [[
                'name' => 'jobs-worker',
                'connection' => 'a',
                'callbacks' => ['jobs.sac' => \app\consumers\JobsConsumer::class],
                'qos' => ['prefetch_count' => 1],
            ]],
        ],
    ],
];
```

先在 Yii 应用中声明队列，再到 RabbitMQ 节点上检查队列参数。

```sh
php yii rabbitmq/declare-queue jobs.sac a
rabbitmqctl list_queues -p /a name arguments
```

在 `jobs.sac` 对应的行应能看到 `x-single-active-consumer` 的值为布尔值 `true`。如果通过 RabbitMQ 管理界面建队列，请先选择虚拟主机 `/a`，在 **Arguments** 中填写同一参数名，类型选 **Boolean**，值填 **true**。所有声明这个队列的应用都应使用一致的队列参数。

#### 启动两个消费者进程并检查主备状态

在**两个终端中分别执行一次**以下命令，或者让进程管理器启动两个实例。

```sh
php yii rabbitmq/consume jobs-worker
```

两个进程都会订阅 `/a` 中的 `jobs.sac`。RabbitMQ 选出其中一个进程接收消息。该进程断开连接时，备用进程自动接手。可选参数 `prefetch_count` 限制活跃进程可以同时持有的未确认消息数。

在 RabbitMQ 节点上运行 `rabbitmqctl list_consumers -p /a queue_name consumer_tag active`。`jobs.sac` 应显示两条订阅记录，其中一条的 `active` 为 `true`，另一条为 `false`。

#### 迁移已有队列

SAC 在创建队列时生效。为普通队列 `jobs` 补上单活参数后原名重新声明，RabbitMQ 会返回 `406 PRECONDITION_FAILED`。要保留积压消息，可以在同一虚拟主机 `/a` 内迁移到新队列，例如 `jobs.sac`。

1. 保留 `jobs` 及其消费者，按上例配置并声明 `jobs.sac`。如果 `jobs-worker` 已被占用，请给新消费者取一个独立的名称。
2. 将连接 `a` 上所有向 `jobs` 投递的生产者切换到新队列。使用默认交换器（`''`）时，把路由键从 `jobs` 改为 `jobs.sac`。使用直连交换器时，先用新的路由键绑定新队列，再切换生产者，保证切换期间每条消息只路由到一个队列。
3. 让旧消费者继续处理积压。所有生产者和重试路由完成切换后，运行 `rabbitmqctl list_queues -p /a name messages_ready messages_unacknowledged`。等 `jobs` 的待投递数和未确认数**均持续为零**，再停止旧消费者。先从应用配置中移除旧消费者、队列和绑定，再下线 RabbitMQ 上的旧资源，避免自动声明将其重新创建。
4. 按前文命令启动两个新消费者进程。等旧队列排空后再启动，可以让旧队列的积压消息先于新任务处理。

## 使用方法

### 消费消息

消费者从队列中取出消息，将 `AMQPMessage` 传给对应回调的 `execute()` 方法。回调类需要实现 `ConsumerInterface`。

```php
<?php

namespace components\rabbitmq;

use mikemadisonweb\rabbitmq\components\ConsumerInterface;
use PhpAmqpLib\Message\AMQPMessage;

class YourConsumer implements ConsumerInterface
{
    public function execute(AMQPMessage $msg)
    {
        $data = $msg->getBody();
        // 在这里处理业务数据，处理成功后确认消息。

        return ConsumerInterface::MSG_ACK;
    }
}
```

RabbitMQ 传输的消息体是字符串。通过本扩展发布数组、整数或可序列化对象时，生产者会调用配置的 `serializer` 并在消息头中做标记。消费者识别该标记后调用 `deserializer`，回调就能读到还原后的数据。

### 回调返回值

| 回调返回值 | RabbitMQ 的处理方式 |
| --- | --- |
| `ConsumerInterface::MSG_ACK` | 确认处理成功，从队列移除这条消息。 |
| `ConsumerInterface::MSG_REJECT` | 拒绝消息，不重新入队。配置了死信交换器时，消息可以进入死信路由。 |
| `ConsumerInterface::MSG_REQUEUE` 或 `false` | 拒绝消息并重新入队，等待再次投递。 |

启用 `proceed_on_exception` 后，如果回调或反序列化抛出异常，消费者会将消息重新入队并释放预取名额。持续失败的消息可能反复投递。对于永久失败的消息，回调可以返回 `MSG_REJECT`，由队列的死信策略处理。请通过回调返回值交由消费者确认或拒绝消息。

消费者与 RabbitMQ 断开连接时，扩展会尝试重连一次，并恢复订阅和 QoS 设置。如果连接在发送消息确认（ACK）时断开，RabbitMQ 可能再次投递该消息。业务回调应能处理重复投递。

`consume()` 退出时会释放本次消费使用的通道。已预取但尚未处理的消息可在下次调用 `consume()` 时再次投递，共享连接上的其他通道仍可使用。

`SIGTERM`、`SIGINT` 和 `SIGHUP` 只记录停止或重启请求，由消费主循环在当前等待或消息处理结束后执行。原有 `idle_timeout` 语义保持不变；配置了正数空闲期限时，空闲期间的信号请求可能要等本次等待结束才生效。停止期限应覆盖等待和任务执行所需时间。

使用外部连接反复创建临时路由对象时，操作完成后调用 `$routing->close()`。该方法只释放路由对象自己的通道，保留共享连接及其其他通道；对象析构时也会尝试释放。关闭后的路由对象仍可再次使用。`reset()` 只用于连接已经失效时丢弃本地拓扑状态，不能替代 `close()`。

### 长任务的心跳与确认超时

长任务需要维持连接，并为处理和确认消息留够时间。例如，下面的消费者每次处理一份报表，请求 15 秒的连接心跳，并将预取数量限制为一条。

```php
'connections' => [[
    'name' => 'batch', 'host' => 'localhost', 'vhost' => '/a',
    'heartbeat' => 15, 'read_write_timeout' => 35, // 单位为秒
]],
'queues' => [['name' => 'reports', 'connection' => 'batch']],
'consumers' => [[
    'name' => 'report-worker', 'connection' => 'batch',
    'callbacks' => ['reports' => \app\components\ReportConsumer::class],
    'qos' => ['prefetch_count' => 1],
]],
```

扩展的 `heartbeat` 默认为 `0`，此时回调执行期间的自动心跳处于关闭状态。更改 Yii 连接配置后，通过进程管理器重启已有消费者，让新连接重新协商心跳。协商值大于零、CLI PHP 已启用 `pcntl` 且 `SIGALRM` 可用时，从 `before_consume` 到 `after_consume` 期间扩展都会按需发送心跳。每条消息处理结束后会恢复原有信号设置。

使用 `--withoutSignals=1`、环境缺少 `pcntl`，或应用已占用 `SIGALRM` 时，可以将回调拆成耗时可控的小步骤，在步骤间调用 `Yii::$app->rabbitmq->getConnection('batch')->checkHeartBeat()`，间隔约为协商心跳值的一半。如果某个原生调用可能阻塞 PHP 超过心跳超时，可在子进程运行该调用，由消费者轮询其完成状态并维持心跳。[RabbitMQ 心跳文档](https://www.rabbitmq.com/docs/heartbeats)

**消息确认超时单独计时。** RabbitMQ 从消息投递时开始计时，直到回调返回并由扩展发送 ACK。心跳保持连接活跃，这段时间仍计入确认期限。消费者的 `idle_timeout` 则控制等待下一条消息的时间。例如，任务最长需要一小时，可以在每个 RabbitMQ 节点的 `rabbitmq.conf` 中设置两小时的确认期限，单位为毫秒。

```ini
consumer_timeout = 7200000
```

从 RabbitMQ 3.12 起，可以通过策略（policy）为指定虚拟主机中的队列单独设置确认超时。以下命令适用于 RabbitMQ 4.2，将 `/a` 下经典队列 `reports` 的期限设为两小时。如果队列已有生效的策略，请将 `consumer-timeout` 加入该策略。[RabbitMQ 4.2 确认超时文档](https://www.rabbitmq.com/docs/4.2/consumers#acknowledgement-timeout)

```sh
rabbitmqctl set_policy -p /a --apply-to classic_queues reports-timeout '^reports$' '{"consumer-timeout":7200000}'
```

RabbitMQ 4.2 的默认确认期限为 30 分钟，超时后会以 `PRECONDITION_FAILED` 关闭通道。从 RabbitMQ 4.3 起，只有仲裁队列（quorum queue）执行消息确认超时，可使用 `--apply-to quorum_queues` 设置相应策略。超时的消息可能重新投递，RabbitMQ 可能取消 AMQP 0-9-1 消费者的订阅或关闭其通道。[RabbitMQ 确认超时文档](https://www.rabbitmq.com/docs/consumers#acknowledgement-timeout)

### Worker 部署配置

生产连接可以从 `heartbeat=15`、`read_write_timeout=35`、`channel_rpc_timeout=5.0` 开始调整。`channel_rpc_timeout` 限制声明、绑定等 RPC 的等待，不能由读写超时代替。启用 `keepalive=true` 还需安装 PHP sockets 扩展。消费者按任务耗时与消息大小设置有限预取，并用 `--memoryLimit`、`--messagesLimit` 定期轮换进程。

生成的 systemd unit 使用独立的启动、停止和重启间隔。每个消费者可以配置：

```php
'systemd' => [
    'workers' => 2,
    'memory_limit' => 256,
    'timeout_stop_sec' => 300,
    'restart_sec' => 5,
],
```

`timeout_stop_sec` 默认 300 秒，应大于任务最大执行时间并留出 ACK 余量；`restart_sec` 默认 5 秒，两者必须为正整数。启动期限为 30 秒。更新配置后重新生成并安装 unit，再执行 `systemctl daemon-reload`；已有服务文件不会自动更新。

### 发布消息

通过 Yii 组件取得命名生产者，调用 `publish()` 指定消息体、交换器和路由键。

```php
$producer = \Yii::$app->rabbitmq->getProducer('YOUR_PRODUCER_NAME');
$msg = serialize(['dataset_id' => 657, 'linked_datasets' => []]);
$producer->publish($msg, 'YOUR_EXCHANGE_NAME', 'YOUR_ROUTING_KEY');
```

这里的 `$msg` 已手动序列化，消费者会收到字符串。直接传入数组时，扩展会自动序列化并标记消息。第三个参数是可选的路由键，使用扇出交换器（`fanout`）时可省略。默认连接采用延迟建立方式，仅在真正使用 RabbitMQ 时才发起连接，普通 HTTP 请求无需每次连接 RabbitMQ。

如果在准备通道或声明交换器、队列和绑定时断线，生产者会先尝试重连一次。如果写入消息时发生异常，`publish()` 会把异常传给调用方。此时消息可能已经送达，重试可能产生重复消息。

### 失败消息重放

`RabbitPublishError::rePublish()` 使用应用组件 `rabbitmqReplayMutex` 互斥执行。未配置时会创建 `yii\mutex\FileMutex`，锁位于 `@runtime/mutex`，进程退出或被终止后由操作系统释放；同一主机上的重放进程必须使用相同的 runtime 路径。多主机共享失败消息表时，配置所有实例共用的数据库互斥锁，例如：

```php
'components' => [
    'rabbitmqReplayMutex' => [
        'class' => \yii\mutex\PgsqlMutex::class, // MySQL 可使用 MysqlMutex
        'db' => 'db',
    ],
],
```

也可在模型配置中用 `'mutex' => 'customMutex'` 指定其他互斥组件。已有 Cache 中的 `rabbit_publish_error` 锁不再参与重放。

新保存的消息头使用带版本信息的 AMQP 编码，保留表、数组、显式字段类型和二进制内容；旧 JSON 记录仍可读取。不支持的消息头值会在保存时抛出异常。滚动升级时先升级所有重放进程，再启用新版写入：旧版重放器不能读取新消息头格式。此变更保留现有发布语义，并未增加 broker confirm；业务仍需处理不确定投递及重复消息。

## 配置项

使用 TLS 时，将连接的 `port` 设为 RabbitMQ 的 TLS 监听端口（通常为 `5671`），并在 `ssl_context` 中填写 PHP TLS 选项，例如 `['cafile' => '/path/to/ca.pem']`。默认连接在首次使用时建立。若沿用 `AMQPSSLConnection::class` 配置，扩展会立即建立 TLS 连接并返回 `AMQPStreamConnection` 实例。若使用 `AMQPLazySSLConnection::class`，则返回 `AMQPLazyConnection` 实例，首次使用时才建立 TLS 连接。

以下默认值来自 [Configuration.php](Configuration.php)。默认连接名为 `default`。

连接 URL 的 query 只接受 `host`、`port`、`user`、`password`、`vhost`、`connection_timeout`、`read_write_timeout`、`keepalive`、`heartbeat`、`channel_rpc_timeout`。query 值覆盖 URL 和数组中的对应字段，然后统一校验。`keepalive` 支持 `true/false/1/0`；端口必须为 1–65535，心跳为 0–65535，超时必须为有限非负秒数，RPC 超时不能大于读写超时。未知、重复、嵌套参数会在配置加载时被拒绝；旧配置中的无效占位参数（如 `?query`、`?lazy=1`）需要移除，延迟连接由 `type` 决定。

```php
<?php
use mikemadisonweb\rabbitmq\Configuration;
use PhpAmqpLib\Connection\AMQPLazyConnection;

$rabbitmq_defaults = [
    'auto_declare' => true,
    'connections' => [
        [
            'name' => Configuration::DEFAULT_CONNECTION_NAME,
            'type' => AMQPLazyConnection::class,
            'url' => null,
            'host' => null,
            'port' => 5672,
            'user' => 'guest',
            'password' => 'guest',
            'vhost' => '/',
            'connection_timeout' => 3,
            'read_write_timeout' => 3,
            'ssl_context' => null,
            'keepalive' => false,
            'heartbeat' => 0,
            'channel_rpc_timeout' => 0.0,
        ],
    ],
    'exchanges' => [
        [
            'name' => null,
            'connection' => null,
            'type' => null,
            'passive' => false,
            'durable' => true,
            'auto_delete' => false,
            'internal' => false,
            'nowait' => false,
            'arguments' => null,
            'ticket' => null,
            'declare' => true,
        ],
    ],
    'queues' => [
        [
            'name' => '',
            'connection' => null,
            'passive' => false,
            'durable' => true,
            'exclusive' => false,
            'auto_delete' => false,
            'nowait' => false,
            'arguments' => null,
            'ticket' => null,
            'declare' => true,
        ],
    ],
    'bindings' => [
        [
            'exchange' => null,
            'connection' => null,
            'queue' => null,
            'to_exchange' => null,
            'routing_keys' => [],
        ],
    ],
    'producers' => [
        [
            'name' => null,
            'connection' => Configuration::DEFAULT_CONNECTION_NAME,
            'safe' => true,
            'content_type' => 'text/plain',
            'delivery_mode' => 2,
            'serializer' => 'serialize',
        ],
    ],
    'consumers' => [
        [
            'name' => null,
            'connection' => Configuration::DEFAULT_CONNECTION_NAME,
            'callbacks' => [],
            'qos' => [
                'prefetch_size' => 0,
                'prefetch_count' => 0,
                'global' => false,
            ],
            'idle_timeout' => 0,
            'idle_timeout_exit_code' => null,
            'proceed_on_exception' => false,
            'deserializer' => 'unserialize',
            'systemd' => [
                'memory_limit' => 0,
                'workers' => 1,
                'timeout_stop_sec' => 300,
                'restart_sec' => 5,
            ],
        ],
    ],
    'logger' => [
        'log' => false,
        'category' => 'application',
        'print_console' => true,
        'system_memory' => false,
    ],
];
```

### 交换器

声明具名交换器时，需要指定名称和类型。

| 参数 | 必填 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `name` | 是 | string | — | 交换器名称。可以使用字母、数字、连字符、下划线、点和冒号。 |
| `type` | 是 | string | — | 交换器类型，可选 `direct`、`fanout`、`topic` 或 `headers`。 |
| `passive` | 否 | boolean | `false` | 被动声明，只检查同名交换器是否存在。存在时服务端返回 `Declare-Ok`，缺失时返回错误。此时仅名称和 `nowait` 生效。同时启用 `nowait` 就无法通过声明响应确认检查结果。 |
| `durable` | 否 | boolean | `true` | 持久化交换器在 RabbitMQ 重启后仍然存在，临时交换器会被清除。 |
| `auto_delete` | 否 | boolean | `false` | 开启后，交换器的最后一条出站绑定被移除时，RabbitMQ 会自动删除这个交换器。 |
| `internal` | 否 | boolean | `false` | 内部交换器只接收其他交换器通过绑定转发的消息。 |
| `nowait` | 否 | boolean | `false` | 开启后，客户端发出声明即可继续发送下一个请求。 |
| `arguments` | 否 | array | `null` | 声明时传给 RabbitMQ 的附加参数。 |
| `ticket` | 否 | integer | `null` | 访问票据。 |

队列的 `arguments` 也可用于指定死信交换器。[php-amqplib 死信配置示例](https://github.com/php-amqplib/php-amqplib/blob/master/demo/queue_arguments.php#L17)

### 队列

队列声明的参数均为可选。省略名称或将名称留空时，RabbitMQ 会生成唯一名称。

| 参数 | 必填 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `name` | 否 | string | `''` | 队列名称。可留空，也可使用字母、数字、连字符、下划线、点和冒号。 |
| `passive` | 否 | boolean | `false` | 被动声明，只检查同名队列是否存在。存在时服务端返回 `Declare-Ok`，缺失时返回错误。 |
| `durable` | 否 | boolean | `true` | 持久化队列在 RabbitMQ 重启后仍然存在，临时队列会被清除。 |
| `auto_delete` | 否 | boolean | `false` | 开启后，曾有消费者订阅的队列在最后一个消费者退出时自动删除。 |
| `exclusive` | 否 | boolean | `false` | 独占队列只允许声明它的连接访问，并随连接关闭而删除。其他连接尝试被动声明时也会收到错误。 |
| `nowait` | 否 | boolean | `false` | 开启后，客户端发出声明即可继续发送下一个请求。 |
| `arguments` | 否 | array | `null` | 声明时传给 RabbitMQ 的附加参数。 |
| `ticket` | 否 | integer | `null` | 访问票据。 |

使用 RabbitMQ 生成的队列名时，保留 `nowait = false`，以便扩展接收实际名称，并在同一连接中用它绑定和消费。每个使用独占匿名队列的消费者进程都会获得自己的队列。

队列或交换器创建后，部分声明参数就会固定。用不同的值重新声明时，RabbitMQ 会返回错误。协议选项、默认值和具体行为可查阅 [AMQP 0-9-1 参考手册](http://www.rabbitmq.com/amqp-0-9-1-reference.html)。

## 从 1.x 升级时的主要变化

扩展在 1.x 之后重写了内部实现。旧版升级时需要留意以下变化。

- 当时发布的重写版本要求 PHP 7.0 或更高版本。当前分支要求 PHP 7.4 或 PHP 8.x。
- 扩展的配置格式发生变化。
- Yii 在应用启动时自动加载扩展组件。[Yii 扩展引导机制](http://www.yiiframework.com/doc-2.0/guide-structure-extensions.html#bootstrapping-classes)
- 支持多种 AMQP 连接类型。
- 连接、生产者、消费者和日志服务在 Yii 依赖注入容器中按单例注册。
- 新增路由组件，用于在 RabbitMQ 中管理交换器、队列和绑定。
- 队列和交换器的默认选项发生变化。
- 控制台命令统一由一个自动注册的控制器提供，并新增了管理交换器、队列和绑定的命令。
- 消息体支持多种 PHP 数据类型。
- 消费者支持通过进程信号控制停止和重启。

## 运行验证测试

在 PHP 7.4 或 8.5 环境中执行 `composer install`。运行完整测试需要 CLI PHP 启用 `pcntl`、`sockets` 和 `pdo_sqlite` 扩展。生成行覆盖率报告还需要启用 Xdebug 的覆盖率模式。

在测试用 RabbitMQ 实例中创建两个可清理的虚拟主机，并为测试账号授予声明和删除测试资源的权限。设置以下环境变量。

| 环境变量 | 值 |
| --- | --- |
| `RABBITMQ_TEST_URL_A`、`RABBITMQ_TEST_URL_B` | 两个测试虚拟主机各自的 AMQP URL。 |
| `RABBITMQ_TEST_TLS_URL` | 第一个测试虚拟主机的 AMQP URL，端口使用 TLS 监听端口，URL 格式与前两项相同。 |
| `RABBITMQ_TEST_TLS_CA` | 测试 CA 证书的 PEM 文件路径。 |

CI 会在临时 RabbitMQ 容器中创建这两个虚拟主机并配置 TLS 监听端口。运行以下命令分别执行测试套件和安装测试。

```sh
XDEBUG_MODE=coverage vendor/bin/phpunit --fail-on-skipped --fail-on-risky --fail-on-warning --coverage-clover build/clover.xml
sh tests/installation/run.sh
```

安装测试会新建一个只安装生产依赖的 Composer 项目，分别启动 Yii 控制台应用和 Web 应用，并验证每个应用都能通过真实的 RabbitMQ 投递和确认消息。

## 许可证

本扩展采用 [MIT 许可证](LICENSE)。
