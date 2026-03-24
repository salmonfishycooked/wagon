## How is a task pushed into queue at expected schedule time?

> We will call "expected schedule time" as "es-time" in this part.

> [!IMPORTANT]
>
> We used Priority Queue to optimize time perplexity.

**Entire Process**:

User submits a task.

Scheduler then:

1. Save the task information to Store.
2. push the task into Priority Queue. (whether current time is or not after es-time)
3. Wake up the scheduler, then check the Priority Queue whether there is any task that have already been due.
4. If any, enqueue them, then calculate next sleeping time based on the top of the Priority Queue. If the Priority Queue is empty, the scheduler should sleep eternally (it will wake up when next task is submitted).

Below is the corresponding Sequence Diagram.

```mermaid
sequenceDiagram
    actor User
    participant Scheduler
    participant PQ as Priority Queue
    participant MQ as Message Queue
    participant Repo as Store

    User->>Scheduler: submit(task)
    Scheduler->>Repo: store(task)
    Scheduler->>PQ: push(taskId, es-time)
    Scheduler->>Scheduler: wake up

    loop Scheduling loop
        Scheduler->>PQ: peek()
        alt top task is due (es-time <= now)
            PQ-->>Scheduler: taskId
            Scheduler->>PQ: pop()
            Scheduler->>MQ: enqueue(taskId)
            Scheduler->>PQ: peek()
        else Priority Queue is empty
            Scheduler->>Scheduler: sleep until next task submit
        else top task is not yet due
            PQ-->>Scheduler: taskId, es-time
            Scheduler->>Scheduler: sleep until es-time
        end
    end
```