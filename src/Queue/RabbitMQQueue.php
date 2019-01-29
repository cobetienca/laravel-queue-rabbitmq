<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue;

use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Interop\Amqp\AmqpContext;
use Interop\Amqp\AmqpMessage;
use Interop\Amqp\AmqpQueue;
use Interop\Amqp\AmqpTopic;
use Interop\Amqp\Impl\AmqpBind;
use Psr\Log\LoggerInterface;
use RuntimeException;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Jobs\RabbitMQJob;

class RabbitMQQueue extends Queue implements QueueContract {
    protected $sleepOnError;
    
    protected $queueOptions;
    protected $exchangeOptions;
    
    private $declaredExchanges = [];
    private $declaredQueues    = [];
    
    /**
     * @var AmqpContext
     */
    private $context;
    private $correlationId;
    
    public function __construct( AmqpContext $context, array $config ) {
        $this->context = $context;
        
        $this->queueOptions              = $config['options']['queue'];
        $this->queueOptions['arguments'] = isset( $this->queueOptions['arguments'] ) ?
            json_decode( $this->queueOptions['arguments'], TRUE ) : [];
        
        $this->exchangeOptions              = $config['options']['exchange'];
        $this->exchangeOptions['arguments'] = isset( $this->exchangeOptions['arguments'] ) ?
            json_decode( $this->exchangeOptions['arguments'], TRUE ) : [];
        
        $this->sleepOnError = $config['sleep_on_error'] ?? 5;
    }
    
    /** @inheritdoc */
    public function size( $queueName = NULL ): int {
        /** @var AmqpQueue $queue */
        list( $queue ) = $this->declareEverything( $queueName );
        
        return $this->context->declareQueue( $queue );
    }
    
    /** @inheritdoc */
    public function push( $job, $data = '', $queue = NULL ) {
        try {
            return $this->pushRaw( $this->createPayload( $job, $data ), $queue, [] );
        } catch ( \Exception $e ) {
            return Queue::connection( 'queue_fallback' )->push( $job, $data, $queue );
        }
    }
    
    /** @inheritdoc */
    public function pushRaw( $payload, $queueName = NULL, array $options = [] ) {
        try {
            /**
             * @var AmqpTopic $topic
             * @var AmqpQueue $queue
             */
            list( $queue, $topic ) = $this->declareEverything( $queueName );
            
            $message = $this->context->createMessage( $payload );
            $message->setRoutingKey( $queue->getQueueName() );
            $message->setCorrelationId( $this->getCorrelationId() );
            $message->setContentType( 'application/json' );
            $message->setDeliveryMode( AmqpMessage::DELIVERY_MODE_PERSISTENT );
            
            if ( isset( $options['attempts'] ) ) {
                $message->setProperty( RabbitMQJob::ATTEMPT_COUNT_HEADERS_KEY, $options['attempts'] );
            }
            
            $producer = $this->context->createProducer();
            if ( isset( $options['delay'] ) && $options['delay'] > 0 ) {
                $producer->setDeliveryDelay( $options['delay'] * 1000 );
            }
            
            $producer->send( $topic, $message );
            
            return $message->getCorrelationId();
        } catch ( \Exception $exception ) {
            $this->reportConnectionError( 'pushRaw', $exception );
            
            return NULL;
        }
    }
    
    /** @inheritdoc */
    public function later( $delay, $job, $data = '', $queue = NULL ) {
        try {
            return $this->pushRaw( $this->createPayload( $job, $data ), $queue, [ 'delay' => $this->secondsUntil( $delay ) ] );
        } catch ( \Exception $e ) {
            return Queue::connection( 'queue_fallback' )->later( $delay, $job, $data, $queue );
        }
    }
    
    /**
     * Release a reserved job back onto the queue.
     *
     * @param  \DateTimeInterface|\DateInterval|int $delay
     * @param  string|object                        $job
     * @param  mixed                                $data
     * @param  string                               $queue
     * @param  int                                  $attempts
     * @return mixed
     */
    public function release( $delay, $job, $data, $queue, $attempts = 0 ) {
        try {
            return $this->pushRaw( $this->createPayload( $job, $data ), $queue, [
                'delay'    => $this->secondsUntil( $delay ),
                'attempts' => $attempts,
            ] );
        } catch ( \Exception $e ) {
            return Queue::connection( 'queue_fallback' )->release( $delay, $job, $data, $queue, $attempts );
        }
        
        
    }
    
    /** @inheritdoc */
    public function pop( $queueName = NULL ) {
        try {
            /** @var AmqpQueue $queue */
            list( $queue ) = $this->declareEverything( $queueName );
            
            $consumer = $this->context->createConsumer( $queue );
            
            if ( $message = $consumer->receiveNoWait() ) {
                return new RabbitMQJob( $this->container, $this, $consumer, $message );
            }
        } catch ( \Exception $exception ) {
            $this->reportConnectionError( 'pop', $exception );
        }
        
        return NULL;
    }
    
    /**
     * Retrieves the correlation id, or a unique id.
     *
     * @return string
     */
    public function getCorrelationId(): string {
        return $this->correlationId ?: uniqid( '', TRUE );
    }
    
    /**
     * Sets the correlation id for a message to be published.
     *
     * @param string $id
     *
     * @return void
     */
    public function setCorrelationId( string $id ) {
        $this->correlationId = $id;
    }
    
    /**
     * @return AmqpContext
     */
    public function getContext(): AmqpContext {
        return $this->context;
    }
    
    /**
     * @param string $queueName
     *
     * @return array [Interop\Amqp\AmqpQueue, Interop\Amqp\AmqpTopic]
     */
    private function declareEverything( string $queueName = NULL ): array {
        $queueName    = $queueName ?: $this->queueOptions['name'];
        $exchangeName = $this->exchangeOptions['name'] ?: $queueName;
        
        $topic = $this->context->createTopic( $exchangeName );
        $topic->setType( $this->exchangeOptions['type'] );
        $topic->setArguments( $this->exchangeOptions['arguments'] );
        if ( $this->exchangeOptions['passive'] ) {
            $topic->addFlag( AmqpTopic::FLAG_PASSIVE );
        }
        if ( $this->exchangeOptions['durable'] ) {
            $topic->addFlag( AmqpTopic::FLAG_DURABLE );
        }
        if ( $this->exchangeOptions['auto_delete'] ) {
            $topic->addFlag( AmqpTopic::FLAG_AUTODELETE );
        }
        
        if ( $this->exchangeOptions['declare'] && !in_array( $exchangeName, $this->declaredExchanges, TRUE ) ) {
            $this->context->declareTopic( $topic );
            
            $this->declaredExchanges[] = $exchangeName;
        }
        
        $queue = $this->context->createQueue( $queueName );
        $queue->setArguments( $this->queueOptions['arguments'] );
        if ( $this->queueOptions['passive'] ) {
            $queue->addFlag( AmqpQueue::FLAG_PASSIVE );
        }
        if ( $this->queueOptions['durable'] ) {
            $queue->addFlag( AmqpQueue::FLAG_DURABLE );
        }
        if ( $this->queueOptions['exclusive'] ) {
            $queue->addFlag( AmqpQueue::FLAG_EXCLUSIVE );
        }
        if ( $this->queueOptions['auto_delete'] ) {
            $queue->addFlag( AmqpQueue::FLAG_AUTODELETE );
        }
        
        if ( $this->queueOptions['declare'] && !in_array( $queueName, $this->declaredQueues, TRUE ) ) {
            $this->context->declareQueue( $queue );
            
            $this->declaredQueues[] = $queueName;
        }
        
        if ( $this->queueOptions['bind'] ) {
            $this->context->bind( new AmqpBind( $queue, $topic, $queue->getQueueName() ) );
        }
        
        return [ $queue, $topic ];
    }
    
    /**
     * @param string     $action
     * @param \Exception $e
     * @throws \Exception
     */
    protected function reportConnectionError( $action, \Exception $e ) {
        /** @var LoggerInterface $logger */
        $logger = $this->container['log'];
        
        $logger->error( 'AMQP error while attempting ' . $action . ': ' . $e->getMessage() );
        
        // If it's set to false, throw an error rather than waiting
        if ( $this->sleepOnError === FALSE ) {
            throw new RuntimeException( 'Error writing data to the connection with RabbitMQ', NULL, $e );
        }
        
        // Sleep so that we don't flood the log file
        sleep( $this->sleepOnError );
    }
}
