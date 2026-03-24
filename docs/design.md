## **Overview**

This is a task scheduling library of Golang.



## **Requirements**

User submits a task to scheduler with a exact schedule time (schedule time means when will user's task be submitted into task queue), and once the task is submitted into the queue, one of workers will acquire it from the task queue, and execute it and store the final result into database.



## **Architecture**

![architecture](./architecture.png)



## **How does wagon execute a task?**

```mermaid
sequenceDiagram
    actor User
    participant Scheduler
    participant Queue
    participant Worker
    participant Store

    User->>Scheduler: submit task
    Scheduler->>Store: store task info

    loop Sleep until the next schedule time
        Scheduler->>Scheduler: check the top of delay queue
    end

    Scheduler->>Queue: enqueue(taskId)

    Worker->>Queue: dequeue()
    Worker->>Store: fetch task info
    Store-->>Worker: return task info

    alt task succeeds
        Worker->>Store: write success result
        Worker->>Queue: ack(taskId)
    else task fails
        Worker->>Store: write failure result
        Worker->>Queue: nack(taskId)
    end

    User->>Scheduler: query task info
    Scheduler->>Store: fetch task info
    Store-->>Scheduler: return task info
    Scheduler-->>User: return task info
```



## Module

### Task

#### State Transition

For now, we've designed four states for task:

- Pending
- Running
- Success
- Failed

The state transition graph is as follows:

```mermaid
stateDiagram-v2
    [*] --> Pending
    Pending --> Running
    Running --> Success
    Running --> Pending
    Running --> Failed
    Success --> [*]
    Failed --> [*]
```



### Scheduler

#### State Transition

- Init
- Running
- Stopped

```mermaid
stateDiagram-v2
    [*] --> Init : New()

    Init --> Running : Start()
    Running --> Stopped : Shutdown()
    Stopped --> [*]
```



### Worker Pool

#### State Transition

- Init
- Running
- Stopped

```mermaid
stateDiagram-v2
    [*] --> Init : New()

    Init --> Running : Start()
    Running --> Stopped : Shutdown()
    Stopped --> [*]
```



### Worker

#### State Transition

- Init
- Running
- Stopped

```mermaid
stateDiagram-v2
    [*] --> Init : New()

    Init --> Running : Start()
    Running --> Stopped : Shutdown()
    Stopped --> [*]
```



### Engine

#### State Transition

- Init
- Running
- Stopped

```mermaid
stateDiagram-v2
    [*] --> Init : New()

    Init --> Running : Start()
    Running --> Stopped : Shutdown()
    Stopped --> [*]
```

