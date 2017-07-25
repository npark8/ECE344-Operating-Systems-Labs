#include "request.h"
#include "server_thread.h"
#include "common.h"
#include "pthread.h"
#include <string.h>

#define EMPTY 0
#define NOT_EMPTY 1
#define HASH_TABLE 512

/* structs */



struct wc{
    char * key;
    int reference_counter;
    pthread_mutex_t lock_rc;
    struct file_data* file;
};

/*holds request information and pointer to next rq*/
struct queue{
    /*socket for request_queue*/
    int socket;
    /*file name, tag for LRU*/
    char* file_name;
    struct wc* cache;
    struct queue *next;
};

struct server {
	int nr_threads;
	int max_requests;
	int max_cache_size;
        int buffer;
        /*global pointers to request queue and thread list*/
        struct queue * rq;
        struct queue * rq_tail;
        pthread_t * threadList;
        /*global lock for accessing sv*/
        pthread_mutex_t lock;
        /*condition variable for full buffer*/
        pthread_cond_t full;
        /*condition variable for empty buffer*/
        pthread_cond_t empty;
	/* add any other parameters you need */
        /*lab5*/
        int current_cache_size;
        pthread_mutex_t lock_cache;
        pthread_mutex_t lock_LRU;
        struct wc* cache;
        struct queue* LRU_queue;
        struct queue* LRU_tail;
        int cache_elem;
};

/* queue functions */

/* add a node at the tail of request queue */
void enqueue(struct queue** q, struct queue* node, struct queue** tail) {
    if (*q == NULL) {
        *q = node;
        *tail = node;
        return;
    }
    struct queue *temp = *tail;
    temp->next = node;
    *tail = node;
}

/*pop the first node at the head of request queue and return pointer to the node*/
struct queue* dequeueFirst(struct queue** q) {
    struct queue* temp = *q;
    if (temp->next != NULL) *q = temp->next;
    else *q = NULL;
    return temp; //need to be freed in the caller code
}

/*create request_queue with max_request nodes*/
void rq_init(struct server *sv, int max_request) {
    int i;
    for(i=0; i< max_request; i++){
        struct queue *node = (struct queue *)malloc(sizeof(struct queue));
        node->socket = -1;
        node->next = NULL;
        enqueue(&sv->rq,node, &sv->rq_tail);
    }
}

/* file_data functions */

/* initialize file data */
static struct file_data *
file_data_init(void)
{
	struct file_data *data;

	data = Malloc(sizeof(struct file_data));
	data->file_name = NULL;
	data->file_buf = NULL;
	data->file_size = 0;
	return data;
}

/* free all file data */
static void
file_data_free(struct file_data *data)
{
	free(data->file_name);
	free(data->file_buf);
	free(data);
}

/* hashtable functions */

unsigned int h1(char *k){
    unsigned int h;
    unsigned char *c;
    
    h = 0;
    for (c=(unsigned char *) k ; *c ; c++)
        h = h*37 + *c;
    return h;
}

unsigned int h2(char *k){
    unsigned int h;
    unsigned char *c;
    
    h = 0;
    for (c=(unsigned char *) k ; *c ; c++)
        h = h*31 + *c;
    return h % 2 == 0 ? h + 1 : h; //returns odd h
}

unsigned int hash(char *key, int index){//, long size){
    return (h1(key) + index * h2(key))  % HASH_TABLE;//size;
}

/*returns an index to either empty/filled location in hash table*/
unsigned int locate(struct wc *table, char *k){//, long size){
    unsigned int i, b;
    
    for(i=0;i<HASH_TABLE;i++){//size;i++){
        b = hash(k,i);//,size);
        if(table[b].key == NULL || strcmp(table[b].key, k)==0){
            break;
        }
    }
    return b; 
}

struct wc *
wc_init()//int max_cache_size)
{
    //long table_size = max_cache_size;
    struct wc *wc;
    wc = Malloc(sizeof(struct wc) * HASH_TABLE);//table_size);

    /*initialize wc's key and data to NULL*/
    unsigned int b;
    for(b = 0; b< HASH_TABLE;b++){//table_size; b++){
        wc[b].key = NULL;
        wc[b].file = NULL;
        pthread_mutex_init(&wc[b].lock_rc, NULL);
    }
    return wc;
}

/* LRU functions */
/* updates a node to MRU after looked up*/
void updateLRU(struct server* sv, char* file_name){//, int command){
    struct queue* temp = sv->LRU_queue;
    struct queue* prev = temp;
    while(temp!=NULL){
        if(strcmp(temp->file_name, file_name) == 0){
            if(temp == sv->LRU_queue) dequeueFirst(&sv->LRU_queue);
            else prev->next = temp->next;
            temp->next = NULL;
            enqueue(&sv->LRU_queue, temp, &sv->LRU_tail);
            break;
        }
        prev = temp;
        temp = temp->next;
    }
}


/*inserts given file to LRU list, MRU gets updated*/
void insertLRU(struct server* sv, char* file_name, struct wc* cache){
    struct queue* node = (struct queue *)malloc(sizeof(struct queue));
    node->file_name = file_name;
    node->next = NULL;
    node->cache = cache;
    enqueue(&sv->LRU_queue, node, &sv->LRU_tail);
}

/*finds LRU cache and returns the node*/
struct queue* findLRU(struct server* sv){
    if(sv->LRU_queue == NULL) return NULL;
    struct queue* temp = sv->LRU_queue;
    struct queue* prev = temp;
    while(temp!=NULL){
        if(temp->cache->reference_counter == 0){
            if(temp == sv->LRU_queue) temp = dequeueFirst(&sv->LRU_queue);
            else prev->next = temp->next;
            temp->next = NULL;
            break;
        }
        prev = temp;
        temp = temp->next;
    }
    return temp;
}

/* caching functions */

enum {UP_RC = 1, DOWN_RC = 2};

void cache_update(struct server* sv, int command, unsigned int hash_index){
    if(hash_index == HASH_TABLE) return;
    if(command == UP_RC){
    pthread_mutex_lock(&sv->lock_LRU);
    updateLRU(sv,sv->cache[hash_index].file->file_name);
    pthread_mutex_unlock(&sv->lock_LRU);
    sv->cache[hash_index].reference_counter++;
    }
    else if (command == DOWN_RC){
        if(sv->cache[hash_index].reference_counter>0){ 
            sv->cache[hash_index].reference_counter--;
        }
    }
}


struct file_data * 
cache_lookup(struct server* sv, char* file_name, unsigned int *index){
    if(file_name == NULL) return NULL;
    unsigned int b;
    //int LRU_update;
    b = locate(sv->cache, file_name);//, sv->max_cache_size);
    if(sv->cache[b].key == NULL) return NULL;
    struct file_data* temp;
    if(strcmp(sv->cache[b].key, file_name)==0) goto out;
    else{
        //probe until find
        while(b<HASH_TABLE && strcmp(sv->cache[b].key, file_name)!=0){
                b++;
        }
        if(b==HASH_TABLE) return NULL;//unlock
    }
out:    
    *index = b;    
    temp = sv->cache[b].file;
    return temp;
}


void 
cache_insert(struct server* sv, struct file_data* file, unsigned int *index){
    //lock
    unsigned int b;
    char * key = file->file_name;
    b = locate(sv->cache, key);//, sv->max_cache_size);
    
    if(sv->cache[b].key == NULL) goto insert;
    else{
        if(strcmp(sv->cache[b].key, key) == 0) return; //unlock
        else{
            while(b<HASH_TABLE && sv->cache[b].key!=NULL){
                b++;//linear probing
            }
            if(b==HASH_TABLE) return;
        }
    }
insert:
    *index = b;
    sv->cache[b].key = key;
    sv->cache[b].file = file;
    sv->cache[b].reference_counter = 1;
    sv->current_cache_size += file->file_size;
    sv->cache_elem++;
    pthread_mutex_lock(&sv->lock_LRU);
    insertLRU(sv,key, &sv->cache[b]);
    pthread_mutex_unlock(&sv->lock_LRU);
}

int cache_evict(struct server* sv, int amount_to_evict){
    int evict_size = 0;
    struct file_data* evict;
    struct queue* temp = NULL;
    pthread_mutex_lock(&sv->lock_LRU);
    while(evict_size < amount_to_evict){
        temp = findLRU(sv); 
        if(temp == NULL) break;
        /*get the file to be deleted without updating LRU status*/
        evict = temp->cache->file; 
        evict_size += evict->file_size;
        temp->cache->file = NULL;
        temp->cache->key = NULL;
        sv->cache_elem--;
        sv->current_cache_size -= evict->file_size;
        file_data_free(evict);
        free(temp); 
        temp = NULL;
    }
    pthread_mutex_unlock(&sv->lock_LRU);
    /*not enough memory, do not insert*/
    if(evict_size < amount_to_evict) return 0;
    return 1;
}


/* static functions */



static void
do_server_request(struct server *sv, int connfd)
{

    if(sv->max_cache_size == 0){
        do_server_request_without_cache(sv,connfd);
        return;
    }
    int ret;
    struct request *rq;
    struct file_data *data;

    data = file_data_init();

    rq = request_init(connfd, data);
    if (!rq) {
            file_data_free(data);
            return;
    }
    // check cache if file has been fetched previously 
    // if no cache yet, fetch from disk and cache the file 
    //pthread_mutex_lock(&sv->lock_cache);
    struct file_data* cache = NULL;
    unsigned int hash_index = HASH_TABLE;
    int lost_race = 0;
    cache = cache_lookup(sv, data->file_name, &hash_index); //rc++
    if(cache!=NULL){ 
        pthread_mutex_lock(&sv->cache[hash_index].lock_rc);
        cache_update(sv, UP_RC, hash_index);
        pthread_mutex_unlock(&sv->cache[hash_index].lock_rc);
        request_set_data(rq,cache);       
        file_data_free(data);
        goto send;
    }
    else{ 
        ret = request_readfile(rq);
        if (!ret){
            request_destroy(rq);
            file_data_free(data);
            return;
        }
        //loses from race, already cached, avoid duplicates 

        if(cache_lookup(sv,data->file_name, &hash_index)!= NULL){
            lost_race = 1;
            goto send;
        }
        //cache file
        int insert = 1;
        int temp_cache_size = sv->current_cache_size + data->file_size;
        if(temp_cache_size > sv->max_cache_size){
            int amount_to_evict = temp_cache_size - sv->max_cache_size;
            if(amount_to_evict > sv->max_cache_size) insert = 0;
            else{ 
                pthread_mutex_lock(&sv->lock_cache);
                insert = cache_evict(sv,amount_to_evict);
                pthread_mutex_unlock(&sv->lock_cache);
            }
        }
        if(insert){
            pthread_mutex_lock(&sv->lock_cache);
            cache_insert(sv, data,&hash_index);//rc=1
            pthread_mutex_unlock(&sv->lock_cache);
        }
    }
send:
    request_sendfile(rq);
    //rc-- 
    if(hash_index<HASH_TABLE){
        pthread_mutex_lock(&sv->cache[hash_index].lock_rc);
        cache_update(sv, DOWN_RC,hash_index);
        pthread_mutex_unlock(&sv->cache[hash_index].lock_rc);
    }
    if(lost_race) file_data_free(data);
    //!!!!!!!!!!!!!!!!!!!!!!!!DISCONNECT FD!!!!!!!!!!!!!!!!!!!!!!!!!
    request_destroy(rq);
}




static void
do_server_request_without_cache(struct server *sv, int connfd)
{
	int ret;
	struct request *rq;
	struct file_data *data;

	data = file_data_init();

	rq = request_init(connfd, data);
	if (!rq) {
		file_data_free(data);
		return;
	}
	ret = request_readfile(rq);
	if (!ret)
		goto out;
        
	request_sendfile(rq);
out:
	request_destroy(rq);
	file_data_free(data);
}


static void * worker(void *sv){
    struct server* s = (struct server*)sv;
    /*read the request queue, acquire lock*/
    while(1){
        pthread_mutex_lock(&s->lock);
        while(s->buffer == s->max_requests){
            /*no request, threads will release lock and sleep until signaled*/
            pthread_cond_wait(&s->empty, &s->lock);
        }
        /*new requests arrived, dequeue first buffer, take care of it*/
        struct queue* emptyRequest = dequeueFirst(&s->rq);
        int socketD = emptyRequest->socket;
        emptyRequest->socket = -1;
        emptyRequest->next = NULL;
        enqueue(&(s->rq), emptyRequest, &(s->rq_tail));
        
        /*buffer is no longer full, signal master thread to accept more buffer*/
        s->buffer++;
        pthread_cond_signal(&s->full);
        pthread_mutex_unlock(&s->lock);
        if(s->max_cache_size > 0) do_server_request(s, socketD);
        else do_server_request_without_cache(s, socketD);
    }
    return NULL;
}

void thread_init(struct server *sv) {
    int i;
    for(i=0; i< sv->nr_threads; i++){
        pthread_create(&(sv->threadList[i]), NULL, worker, (void *)sv );
    }
}

/* entry point functions */

struct server *
server_init(int nr_threads, int max_requests, int max_cache_size)
{
	struct server *sv;

	sv = Malloc(sizeof(struct server));
	sv->nr_threads = nr_threads;
	sv->max_requests = max_requests;
	sv->max_cache_size = max_cache_size;
        sv->buffer = max_requests;
        sv->rq = NULL;
        sv->rq_tail = NULL;
        sv->threadList = NULL;
        
        pthread_mutex_init(&sv->lock, NULL);
        
        
        /* Lab 4: create queue of max_request size when max_requests > 0 */
        if(max_requests > 0){
            rq_init(sv, max_requests);
        }
        /* Lab 4: create worker threads when nr_threads > 0 */
        if(nr_threads > 0){
            sv->threadList = Malloc(sizeof(pthread_t) * nr_threads);
            thread_init(sv);
        } 
	
	/* Lab 5: init server cache and limit its size to max_cache_size */
        if(max_cache_size > 0){
            sv->cache_elem = 0;
            sv->current_cache_size = 0;
            sv->cache = wc_init();//max_cache_size);
            sv->LRU_queue = NULL;
            sv->LRU_tail = NULL;
            pthread_mutex_init(&sv->lock_cache, NULL);
        }
	return sv;
}

void
server_request(struct server *sv, int connfd)
{
	if (sv->nr_threads == 0) { /* no worker threads */
		do_server_request(sv, connfd);
	} else {
		/*  Save the relevant info in a buffer and have one of the
		 *  worker threads do the work. */
            struct queue * temp = NULL;
            pthread_mutex_lock(&sv->lock);
            /*buffer full, master thread will wait*/
            while(sv->buffer == 0){
                pthread_cond_wait(&sv->full, &sv->lock);
            }
            /*some buffers have been taken cared, master thread wakes up*/
            temp = sv->rq;
            while(temp!= NULL && temp->socket != -1){
                temp = temp->next;
            }
            temp->socket = connfd;
            /*wake up a thread who is not currently working*/
            pthread_cond_broadcast(&sv->empty);
            sv->buffer--;
            pthread_mutex_unlock(&sv->lock);
        }
}
