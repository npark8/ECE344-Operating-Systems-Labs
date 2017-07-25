#include <assert.h>
#include <stdlib.h>
#include <ucontext.h>
#include "thread.h"
#include "interrupt.h"
#include <stdint.h>

enum {
    READY = 0, RUNNING = 1, NEW = 2, EXIT_QUEUED = 3, WAIT_QUEUED = 4,
    AVAILABLE = 5, ACQUIRED = 6, NONE = -1
};

/* This is the thread control block */
struct thread {
    Tid id;
    struct ucontext context;
    void *stackPtr; //malloc addr of allocated stack mem
    int flag;

    /* ... Fill this in ... */
};

struct queue {
    struct thread* t;
    struct queue* next;
};

//tracks number of threads created
Tid numThreads = 0;

//head pointer for ready & exit queue
//will be passed as double pointers to queue helper functions
//to prevent local modification
struct queue *rq = NULL;
struct queue *eq = NULL;

//array of thread objects
struct thread threadList[THREAD_MAX_THREADS];

//used for multiple purposes
struct queue * threadToRun = NULL;

//holds the current thread's tid
Tid currId = 0;

//function that inserts a new node at the tail 

void enqueue(struct queue** q, struct queue* node) {
    int enabled = interrupts_set(0);
    if (*q == NULL) {
        *q = node;
        return;
    }
    struct queue *temp = *q;
    while (temp->next != NULL) {
        temp = temp->next;
    }
    temp->next = node;
    interrupts_set(enabled);
}

//function that finds a node with matching tid, returns a pointer to it

struct queue * findNode(struct queue** q, Tid tid) {
    int enabled = interrupts_set(0);
    struct queue * temp = *q;
    while (temp != NULL) {
        if (tid == temp->t->id) {
            break;
        }
        temp = temp->next;
    }
    interrupts_set(enabled);
    return temp;
}

//dequeue first node and returns a pointer to it

struct queue* dequeueFirst(struct queue** q) {
    int enabled = interrupts_set(0);
    struct queue* temp = *q;
    if (temp->next != NULL) *q = temp->next;
    else *q = NULL;
    interrupts_set(enabled);
    return temp;
}

//function that dequeues a node with matching tid, returns 1 upon success

int dequeue(struct queue** q, Tid tid) {
    int enabled = interrupts_set(0);
    struct queue *prev, *temp;
    prev = *q;
    temp = *q;
    int found = 0;
    while (temp != NULL) {
        if (temp->t->id == tid) {
            if (temp == *q) {
                dequeueFirst(q);
                threadToRun = temp;
                interrupts_set(enabled);
                return 1;
            } else found = 1;
            break;
        }
        prev = temp;
        temp = temp->next;
    }
    if (found == 1) {
        threadToRun = temp;
        prev->next = temp->next; //dequeue
        interrupts_set(enabled);
        return found;
    }
    interrupts_set(enabled);
    return found;
}

//destroy as much nodes as possible, except the current thread

void destroyQueue(struct queue** q) {
    int enabled = interrupts_set(0);
    if (*q == NULL) {
        interrupts_set(enabled);
        return;
    }

    struct queue* curr = *q;
    while (curr != NULL) {
        if (curr->t->id != currId) {
            dequeue(q, curr->t->id); //dequeued node is pointed by threadToRun
            threadToRun->t->flag = NEW;
            free(threadToRun->t->stackPtr);
            free(threadToRun);
            threadToRun = NULL;
        }
        curr = curr->next;
    }
    interrupts_set(enabled);
}

/* perform any initialization needed by your threading system */
void
thread_init(void) {
    int enabled = interrupts_set(0);
    //initializing variables
    int i;
    for (i = 0; i < THREAD_MAX_THREADS; i++) {
        threadList[i].flag = NEW;
        threadList[i].stackPtr = NULL;
    }
    threadList[0].id = 0;
    threadList[0].flag = RUNNING;

    numThreads++;
    currId = 0;
    interrupts_set(enabled);
    /* your optional code here */
}

/* return the thread identifier of the currently running thread */
Tid
thread_id() {
    return currId;
}

/* thread starts by calling thread_stub. The arguments to thread_stub are the
 * thread_main() function, and one argument to the thread_main() function. */
void
thread_stub(void (*thread_main)(void *), void *arg) {
    Tid ret;
    int enabled = interrupts_set(1);
    thread_main(arg); // call thread_main() function with arg
    ret = thread_exit();
    interrupts_set(enabled);
    // we should only get here if we are the last thread. 
    assert(ret == THREAD_NONE);
    // all threads are done, so process should exit
    exit(0);
}

/* thread_create should create a thread that starts running the function
 * fn(arg). Upon success, return the thread identifier. On failure, return the
 * following:
 *
 * THREAD_NOMORE: no more threads can be created.
 * THREAD_NOMEMORY: no more memory available to create a thread stack. */
Tid
thread_create(void (*fn) (void *), void *parg) {
    int enabled = interrupts_set(0);
    //checking if reached the max thread capacity
    if (numThreads >= THREAD_MAX_THREADS) return THREAD_NOMORE;

    //allocating new mem block of stack, also checking if reached max mem capacity
    void *newStackPtr;
    newStackPtr = (void*) malloc(THREAD_MIN_STACK);
    //malloc failed - no memory available
    if (newStackPtr == NULL)
        return THREAD_NOMEMORY;

    //finding an available index for the new thread either from threadList
    Tid newIndex;
    int i;
    for (i = 0; i < THREAD_MAX_THREADS; i++) {
        if (threadList[i].flag == NEW) {
            newIndex = i;
            break;
        }
    }
    threadList[newIndex].id = newIndex;
    threadList[newIndex].flag = READY;
    threadList[newIndex].stackPtr = newStackPtr;
    numThreads++;

    //manipulating PC, SP, Parameter registers
    //frame pointer has to be allined to 16 bytes
    //calculate offset that will be subtracted from the SP address
    //since stack pushes the frame pointer down at the beginning
    //it creates a 8 bytes difference which leads to misallignment
    //therefore add 8 bytes to reinitiallize
    int offset = (uintptr_t) (newStackPtr + THREAD_MIN_STACK) % 16;
    //create a valid copy of context (i.e. use current thread's context)
    //copy the current context to new thread's context
    getcontext(&(threadList[newIndex].context));
    //make the stack pointer to point to the top of the mem block
    threadList[newIndex].context.uc_mcontext.gregs[REG_RSP] =
            (long long int) ((newStackPtr + THREAD_MIN_STACK - offset + 8));
    //make the PC to point to thread_stub
    threadList[newIndex].context.uc_mcontext.gregs[REG_RIP] =
            (long long int) (&thread_stub);
    //pass the first argument
    threadList[newIndex].context.uc_mcontext.gregs[REG_RDI] =
            (long long int) (fn);
    //pass the second argument
    threadList[newIndex].context.uc_mcontext.gregs[REG_RSI] =
            (long long int) (parg);

    //put the new thread in the ready queue
    struct queue *node;
    node = (struct queue *) malloc(sizeof (struct queue));
    node->t = &threadList[newIndex];
    node->next = NULL;
    enqueue(&rq, node);
    interrupts_set(enabled);
    return newIndex;
}

/* thread_yield should suspend the calling thread and run the thread with
 * identifier tid. The calling thread is put in the ready queue. tid can be
 * identifier of any available thread or the following constants:
 *
 * THREAD_ANY:	   run any thread in the ready queue.
 * THREAD_SELF:    continue executing calling thread, for debugging purposes.
 *
 * Upon success, return the identifier of the thread that ran. The calling
 * thread does not see this result until it runs later. Upon failure, the
 * calling thread continues running, and returns the following:
 *
 * THREAD_INVALID: identifier tid does not correspond to a valid thread.
 * THREAD_NONE:    no more threads, other than the caller, are available to
 *		   run. this can happen is response to a call with tid set to
 *		   THREAD_ANY. */
Tid
thread_yield(Tid want_tid) {
    int enabled = interrupts_set(0);
    destroyQueue(&eq); //destroy any exited thread
    //not stored in registers, a flag for telling if getcontext returned once/twice
    volatile int setcontext_called = 0;

    //case checking - tid not in valid range
    if ((want_tid < 0 && want_tid != THREAD_SELF && want_tid != THREAD_ANY)
            || want_tid >= THREAD_MAX_THREADS) {
        interrupts_set(enabled);
        return THREAD_INVALID;
    }
    //case checking - yield called upon itself    
    if (want_tid == THREAD_SELF || want_tid == currId) {
        getcontext(&(threadList[currId].context));
        if (setcontext_called == 0) {
            setcontext_called = 1;
            setcontext(&(threadList[currId].context));
        }
        interrupts_set(enabled);
        return currId;
    }

    //find a new threadToRun
    //THREAD_ANY will follow FIFO policy
    if (want_tid == THREAD_ANY) {
        //rq empty, no threads available
        if (rq == NULL) {
            interrupts_set(enabled);
            return THREAD_NONE;
        }
        threadToRun = dequeueFirst(&rq);
        want_tid = threadToRun->t->id;
    }        //find the node with matching tid in rq
    else {
        if (rq == NULL) {
            interrupts_set(enabled);
            return THREAD_INVALID;
        }
        int found = dequeue(&rq, want_tid);
        if (found == 0) {
            interrupts_set(enabled);
            return THREAD_INVALID;
        }
    }

    //at this point threadToRun is updated with appropriate thread
    //make sure the node is completely isolated from the rq
    threadToRun->next = NULL;

    //checking if called from thread_exit() or thread_sleep()
    int enqueued = 0;
    if (threadList[currId].flag == EXIT_QUEUED) {
        enqueued = 1;
        threadList[currId].flag = NEW;
    } else if (threadList[currId].flag == WAIT_QUEUED) {
        enqueued = 1;
    } else threadList[currId].flag = READY;

    assert(currId != threadToRun->t->id);
    //save currThread's context  (e.g. PC counter to continue from here for next run)
    getcontext(&(threadList[currId].context));
    if (setcontext_called == 0) {
        if (enqueued == 0) {
            struct queue* node = (struct queue*) malloc(sizeof (struct queue));
            node->next = NULL;
            node->t = &threadList[currId];
            enqueue(&rq, node);
        }
        currId = threadToRun->t->id;
        threadList[currId].flag = RUNNING;
        //now free the dequeued node
        threadToRun->t = NULL;
        free(threadToRun);
        threadToRun = NULL;
        setcontext_called = 1;
        setcontext(&(threadList[currId].context));
    }
    interrupts_set(enabled);

    return want_tid;
}

/* thread_exit should ensure that the current thread does not run after this
 * call, i.e., this function should not return (unless it fails as described
 * below). In the future, a new thread should be able to reuse this thread's
 * identifier.
 * 
 * Upon failure, the current thread continues running, and returns the
 * following:
 *
 * THREAD_NONE:	   there are no other threads that can run, i.e., this is the
 *		   last thread in the system. */
Tid
thread_exit() {
    int enabled = interrupts_set(0);
    if (rq == NULL) {
        interrupts_set(enabled);
        return THREAD_NONE;
    }
    //set flag to bypass yield

    threadList[currId].flag = EXIT_QUEUED;
    struct queue* node = (struct queue*) malloc(sizeof (struct queue));
    node->next = NULL;
    node->t = &threadList[currId];
    enqueue(&eq, node);
    //the threads in eq will never run again, can be treated as a dead thread
    //even if not freed yet
    numThreads--;
    interrupts_set(enabled);
    thread_yield(THREAD_ANY);
    return THREAD_INVALID;
}

/* Kill another thread whose identifier is tid. When a thread is killed, it
 * should not run any further. The calling thread continues to
 * execute and receives the result of the call. tid can be the identifier of any
 * available thread.
 *
 * Upon success, return the identifier of the thread that was killed. Upon
 * failure, return the following:
 *
 * THREAD_INVALID: identifier tid does not correspond to a valid thread, or it
 * is the current thread.
 */
Tid
thread_kill(Tid tid) {
    int enabled = interrupts_set(0);
    if (tid == currId || tid == THREAD_ANY || rq == NULL) {
        interrupts_set(enabled);
        return THREAD_INVALID;
    }
    struct queue *temp = NULL;
    temp = findNode(&rq, tid);
    if (temp != NULL) {
        dequeue(&rq, tid);
        threadToRun->next = NULL; //isolate the node from rq
        enqueue(&eq, threadToRun);
        //set flag for id reuse
        threadList[tid].flag = NEW;
        numThreads--;
        interrupts_set(enabled);
        return tid;
    } else {
        interrupts_set(enabled);
        return THREAD_INVALID;
    }
}

/*******************************************************************
 * Important: The rest of the code should be implemented in Lab 3. *
 *******************************************************************/

/* This is the wait queue structure */
struct wait_queue {
    struct thread* t;
    struct wait_queue* next;
    /* ... Fill this in ... */
};

void enqueueWait(struct wait_queue* q, struct wait_queue* node) {
    int enabled = interrupts_set(0);
    if (q == NULL || q->next == NULL) {
        if (q->next == NULL) q->next = node;
        interrupts_set(enabled);
        return;
    }
    struct wait_queue *temp = q->next;
    while (temp->next != NULL) {
        temp = temp->next;
    }
    temp->next = node;
    interrupts_set(enabled);
}

//dequeue first node and returns a pointer to it

struct wait_queue* dequeueFirstWait(struct wait_queue* q) {
    int enabled = interrupts_set(0);
    struct wait_queue* temp = q->next;
    if (q->next->next != NULL) q->next = q->next->next;
    else q->next = NULL;
    interrupts_set(enabled);
    return temp;
}

struct wait_queue *
wait_queue_create() {
    int enabled = interrupts_set(0);
    struct wait_queue *wq;

    wq = (struct wait_queue*)malloc(sizeof (struct wait_queue));
    assert(wq);

    wq->t = NULL;
    wq->next = NULL;
    interrupts_set(enabled);
    return wq;
}

void
wait_queue_destroy(struct wait_queue *wq) {
    //need to figure out where the dequeued node would be stored at... rq?
    // or do not free if wq is not empty..
    int enable = interrupts_set(0);
    if (wq->next == NULL) {
        free(wq);
        interrupts_set(enable);
        return;
    }
}

Tid
thread_sleep(struct wait_queue *queue) {
    int enabled = interrupts_set(0);
    if (queue == NULL) {
        interrupts_set(enabled);
        return THREAD_INVALID;
    }
    if (rq == NULL) {
        interrupts_set(enabled);
        return THREAD_NONE;
    }
    threadList[currId].flag = WAIT_QUEUED;
    struct wait_queue *wq = (struct wait_queue*) malloc(sizeof (struct wait_queue));
    wq->t = &threadList[currId];
    wq->next = NULL;
    enqueueWait(queue, wq);
    interrupts_set(enabled);
    Tid ret = thread_yield(THREAD_ANY);
    return ret;
}

/* when the 'all' parameter is 1, wakeup all threads waiting in the queue.
 * returns whether a thread was woken up on not. */
int
thread_wakeup(struct wait_queue *queue, int all) {
    int enabled = interrupts_set(0);
    if (queue == NULL || queue->next == NULL) {
        interrupts_set(enabled);
        return 0;
    }
    int numWakeUp = 0;
    struct wait_queue * temp;
    struct wait_queue * curr = queue->next;
    while (curr != NULL) {
        temp = dequeueFirstWait(queue);
        numWakeUp++;
        temp->t->flag = READY;
        struct queue* node = (struct queue*) malloc(sizeof(struct queue));
        node->t = temp->t;
        node->next = NULL;
        enqueue(&rq, node);
        curr = curr->next;
        temp->t = NULL;
        temp->next = NULL;
        free(temp);
        if(all == 0) break;
    }
    interrupts_set(enabled);
    return numWakeUp;
}

struct lock {
    struct wait_queue * wq;
    int flag; //either AVAILABLE or AQUIRED
    Tid callerId; //stores current caller on this lock
    /* ... Fill this in ... */
};

struct lock *
lock_create() {
    int enabled = interrupts_set(0);
    struct lock *lock;

    lock = (struct lock *)malloc(sizeof (struct lock));
    assert(lock);

    lock->wq = wait_queue_create();
    lock->flag = AVAILABLE;
    lock->callerId = NONE;
    interrupts_set(enabled);

    return lock;
}

void
lock_destroy(struct lock *lock) {
    assert(lock != NULL);
    int enabled = interrupts_set(0);
    if (lock->flag == ACQUIRED || lock->wq->next!=NULL) {
        interrupts_set(enabled);
        return;
    }
    wait_queue_destroy(lock->wq);
    free(lock);
    interrupts_set(enabled);
}

void
lock_acquire(struct lock *lock) {
    assert(lock != NULL);
    int enabled = interrupts_set(0);      
    /*If some other thread is using this lock, wait until released*/
    
    while (lock->flag == ACQUIRED) { //PC SHOULD BE HELD HERE UNTIL ACQUIRES LOCK
        thread_sleep(lock->wq);
    }
    //now acquire lock
    lock->flag = ACQUIRED;
    lock->callerId = currId;
    interrupts_set(enabled);
}

void
lock_release(struct lock *lock) {
    assert(lock != NULL);
    int enabled = interrupts_set(0);
    /*check if curr thread aquired this lock*/
    if (currId == lock->callerId) {
        lock->flag = AVAILABLE;
        lock->callerId = NONE;
        /*wake up all threads waiting to acquire this lock*/
        thread_wakeup(lock->wq, 1);
    }
    interrupts_set(enabled);
    return;
}

struct cv {
    /* ... Fill this in ... */
    struct wait_queue* wq;
};

struct cv *
cv_create() {
    struct cv *cv;

    cv = malloc(sizeof (struct cv));
    assert(cv);

    cv->wq = wait_queue_create();

    return cv;
}

void
cv_destroy(struct cv *cv) {
    assert(cv != NULL);
    int enabled = interrupts_set(0);
    if (cv->wq->next!=NULL) {
        interrupts_set(enabled);
        return;
    }
    wait_queue_destroy(cv->wq);
    free(cv);
    interrupts_set(enabled);
}

void
cv_wait(struct cv *cv, struct lock *lock) {
    assert(cv != NULL);
    assert(lock != NULL);
    int enabled = interrupts_set(0);
    if(lock->callerId == currId){
        lock_release(lock);
        thread_sleep(cv->wq);
        lock_acquire(lock);
    }
    interrupts_set(enabled);
}

void
cv_signal(struct cv *cv, struct lock *lock) {
    assert(cv != NULL);
    assert(lock != NULL);
    int enabled = interrupts_set(0);
    if(lock->callerId == currId){
        thread_wakeup(cv->wq,0);
    }
    interrupts_set(enabled);
    return;
}

void
cv_broadcast(struct cv *cv, struct lock *lock) {
    assert(cv != NULL);
    assert(lock != NULL);
    int enabled = interrupts_set(0);
    if(lock->callerId == currId){
        thread_wakeup(cv->wq,1);
    }
    interrupts_set(enabled);
    return;
}
