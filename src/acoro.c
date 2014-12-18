/* © Copyright 2012 jingmi. All Rights Reserved.
 *
 * +----------------------------------------------------------------------+
 * | Implementation of coroutine                                          |
 * +----------------------------------------------------------------------+
 * | Author: nosqldev@gmail.com                                           |
 * +----------------------------------------------------------------------+
 * | Created: 2012-02-02 11:10                                            |
 * +----------------------------------------------------------------------+
 */

/* TODO:
 * 0. Fix CurrLoop bug!!!
 * 1. timeout connect
 * 2. mutiple manager_sem
 * 3. sleep
 *
 * 2013-12-27
 * 1. Separated thread for disk & sock IO
 *
 * 2014-03-01
 * 1. Add logger thread
 * 2. Add feature: switch current context to background in user's coroutine [done]
 * 3. join method: add arg for join method in crt_attr_setXXX() is a good choice
 *
 * 2014-03-09
 * 1. Improve communication performance: better pipe
 *
 * 2014-03-26
 * 1. yield interface [done]
 *
 * 2014-03-29
 * 1. priority sem_post [done]
 * 2. channel
 * 3. reduce 'case procedure' in new_manager()
 * 4. priority task queues
 * 5. rename CORO_* to CRT_* [done]
 *
 * 2014-03-30
 * 1. remove bsdqueue.h in header file
 * 2. crt_sem_timedwait()
 */

/* {{{ include header files */

#include <string.h>
#include <assert.h>
#include <semaphore.h>
#include <fcntl.h>
#include <sched.h>

#include "acoro.h"
#include "ev.h"

/* }}} */
/* {{{ hooks & predefined subs */

#ifndef acoro_malloc
#define acoro_malloc malloc
#endif /* ! acoro_malloc */

#ifndef acoro_free
#define acoro_free free
#endif /* ! acoro_free */

#define Lock(name) list_lock(coroutine_env.name)
#define UnLock(name) list_unlock(coroutine_env.name)

#define Shift(name, ptr) do { Lock(name); list_shift(coroutine_env.name, ptr); UnLock(name); } while (0)
#define Push(name, ptr) do { Lock(name); list_push(coroutine_env.name, ptr); UnLock(name); } while (0)
#define Unshift(name, ptr) do { Lock(name); list_unshift(coroutine_env.name, ptr); UnLock(name); } while (0)
#define Remove(name, ptr) do { Lock(name); list_remove(coroutine_env.name, ptr); UnLock(name); } while (0)

#define IO_WATCHER_REF_TASKPTR(io_w_ptr)    (list_item_ptr(task_queue))((char *)io_w_ptr - __offset_of(struct list_item_name(task_queue), ec.sock_watcher))
#define TIMER_WATCHER_REF_TASKPTR(tm_w_ptr) (list_item_ptr(task_queue))((char *)tm_w_ptr - __offset_of(struct list_item_name(task_queue), ec.sock_timer))

/* }}} */
/* {{{ config */

/* TODO define MAX_* instead */

#ifndef MANAGER_CNT
/* ONLY 1 manager thread now */
#define MANAGER_CNT (1)
#endif /* ! MANAGER_CNT */

#ifndef BACKGROUND_WORKER_CNT
#define BACKGROUND_WORKER_CNT (4)
#endif /* ! BACKGROUND_WORKER_CNT */

#ifndef COROUTINE_STACK_SIZE
#define COROUTINE_STACK_SIZE (1024 * 32)
#endif /* ! COROUTINE_STACK_SIZE */

/* }}} */
/* {{{ enums & structures */

/* TODO
 * 1. rename *sock* to *tcp*
 */
enum action_t
{
    act_new_coroutine,
    act_finished_coroutine,
    act_sched_yield,

    act_disk_open,
    act_disk_read,
    act_disk_write,

    act_sock_read,
    act_sock_write,
    act_sock_done,

    act_disk_open_done,
    act_disk_read_done,
    act_disk_write_done,

    act_tcp_blocked_connect,
    act_tcp_blocked_connect_done,
    act_tcp_timeout_connect,
    act_tcp_timeout_connect_done,

    act_msleep,
    act_msleep_done,

    act_bg_run,
    act_bg_run_done,

    act_tcp_accept,
    act_tcp_accept_done,

    act_sem_activation,
};

struct init_arg_s
{
    launch_routine_t func;
    size_t stack_size;
    void *func_arg;
};

struct open_arg_s
{
    const char *pathname;
    int flags;
    mode_t mode;
};

struct io_arg_s
{
    int timeout_ms; /* for sock io only */

    int fd;
    void *buf;
    size_t count;
};

struct connect_arg_s
{
    int timeout_ms; /* for timeout connect only */

    in_addr_t ip;
    in_port_t port;
    int fd;
};

/**
 * @brief Design for general purpose
 */
struct general_arg_s
{
    uint32_t u32;
    float    f;
    uint64_t u64;
    double   d;
    void *   ptr;
};

struct routine_arg_s
{
    bg_routine_t func;
    void *arg;
    void *result;
};

struct tcp_accept_arg_s
{
    int sockfd;
    struct sockaddr *addr;
    socklen_t *addrlen;
};

union args_u
{
    struct init_arg_s init_arg;
    struct open_arg_s open_arg;
    struct io_arg_s io_arg;
    struct connect_arg_s connect_arg;
    struct general_arg_s general_arg;
    struct routine_arg_s routine_arg;
    struct tcp_accept_arg_s tcp_accept_arg;
};

struct ev_controller_s
{
    ev_io sock_watcher;
    ev_timer sock_timer;
    ssize_t have_io;
    ssize_t need_io;
};

list_def(task_queue);
struct list_item_name(task_queue)
{
    enum action_t action;
    union args_u args;
    struct
    {
        ssize_t val;
        int err_code;
    } ret;

    ucontext_t task_context;
    void *stack_ptr;
    struct ev_controller_s ec;

    list_next_ptr(task_queue);
};

struct coroutine_env_s
{
    pthread_t manager_tid[MANAGER_CNT];
    pthread_t *background_worker_tid; /* default: as large as BACKGROUND_WORKER_CNT */

    size_t background_worker_count; /* default is BACKGROUND_WORKER_CNT */

    sem_t manager_sem[MANAGER_CNT];
    int *pipe_channel; /* default: as large as double BACKGROUND_WORKER_CNT */

    struct
    {
        volatile uint64_t cid; /* coroutine id */
        volatile uint64_t ran; /* how many coroutines had been ran */
        volatile uint64_t bg_worker_id;
    } info;

    /* TODO manager_context should be an array */
    ucontext_t manager_context;
    list_item_ptr(task_queue) curr_task_ptr[MANAGER_CNT];
    struct
    {
        struct ev_loop *loop;
        ev_io watcher;
    } *worker_ev; /* default: as many as BACKGROUND_WORKER_CNT */

    list_head_ptr(task_queue) todo_queue;  /* in manager thread context */
    list_head_ptr(task_queue) doing_queue; /* in background thread context */
};

/* }}} */

__thread volatile uint64_t g_thread_id;
struct coroutine_env_s coroutine_env;

/* {{{ manager thread */

#ifdef __x86_64__

static void
call_wrapper(uint32_t func_low, uint32_t func_high, uint32_t args_low, uint32_t args_high)
{
    launch_routine_t func_ptr = (launch_routine_t)((uint64_t)func_high << 32 | func_low);
    uintptr_t args = ((uintptr_t)args_high) << 32 | args_low;

    (*func_ptr)((void *)args);
}

#endif /* __x86_64__ */

static void *
new_manager(void *arg)
{
    list_item_ptr(task_queue) task_ptr;

    g_thread_id = (intptr_t)arg;

    for ( ; ; )
    {
        assert(g_thread_id == 0); /* incorrect when we have more than 1 manager threads */
        sem_wait(&coroutine_env.manager_sem[g_thread_id]);
        Shift(todo_queue, task_ptr);
        assert(task_ptr != NULL);
        if (task_ptr == NULL)
            continue; /* should never reach here */

loop:
        switch (task_ptr->action)
        {
        case act_new_coroutine:
            getcontext(&coroutine_env.manager_context);
            getcontext(&task_ptr->task_context);
            task_ptr->stack_ptr = acoro_malloc(task_ptr->args.init_arg.stack_size);
            task_ptr->task_context.uc_stack.ss_sp = task_ptr->stack_ptr;
            task_ptr->task_context.uc_stack.ss_size = task_ptr->args.init_arg.stack_size;
            task_ptr->task_context.uc_link = &coroutine_env.manager_context;
#ifdef __x86_64__

#pragma pack(push)
#pragma pack(1)
            struct transporter_s
            {
                uint32_t tmp_store1; /* low */
                uint32_t tmp_store2; /* high */
            };
            struct transporter_s transporter_func, transporter_arg;
            memcpy(&transporter_func, &task_ptr->args.init_arg.func, sizeof task_ptr->args.init_arg.func);
            memcpy(&transporter_arg, &task_ptr->args.init_arg.func_arg, sizeof task_ptr->args.init_arg.func_arg);

            typedef void (*makecontenxt_func_ptr_t)();
            makecontext(&task_ptr->task_context,
                        (makecontenxt_func_ptr_t)call_wrapper,
                        4,
                        transporter_func.tmp_store1,
                        transporter_func.tmp_store2,
                        transporter_arg.tmp_store1,
                        transporter_arg.tmp_store2);
#pragma pack(pop)
#else /* __x86_32__ */
            makecontext(&task_ptr->task_context,
                        (void(*)(void))task_ptr->args.init_arg.func,
                        1,
                        task_ptr->args.init_arg.func_arg);
#endif /* ! __x86_64__ */
            coroutine_env.curr_task_ptr[ g_thread_id ] = task_ptr;
            swapcontext(&coroutine_env.manager_context, &task_ptr->task_context);
            if (task_ptr->action == act_finished_coroutine)
                goto loop;
            break;

        case act_finished_coroutine:
            acoro_free(coroutine_env.curr_task_ptr[ g_thread_id ]->stack_ptr);
            acoro_free(coroutine_env.curr_task_ptr[ g_thread_id ]);
            __sync_add_and_fetch(&coroutine_env.info.ran, 1);
            break;

        case act_sched_yield:
        case act_disk_open_done:
        case act_disk_read_done:
        case act_disk_write_done:
        case act_sock_done:
        case act_tcp_blocked_connect_done:
        case act_tcp_timeout_connect_done:
        case act_msleep_done:
        case act_bg_run_done:
        case act_tcp_accept_done:
        case act_sem_activation:
            coroutine_env.curr_task_ptr[ g_thread_id ] = task_ptr;
            swapcontext(&coroutine_env.manager_context, &task_ptr->task_context);
            if (task_ptr->action == act_finished_coroutine)
                goto loop;
            break;

        default:
            /* should never reach here */
            abort();
            break;
        }
    }

    return NULL;
}

/* }}} */
/* {{{ background worker thread */

#define CurrLoop (coroutine_env.worker_ev[ g_thread_id ].loop)
#define CurrWatcher (coroutine_env.worker_ev[ g_thread_id ].watcher)
#define CurrReadPipe (coroutine_env.pipe_channel[g_thread_id * 2])

static inline void
coroutine_notify_manager(list_item_ptr(task_queue) task_ptr)
{
    uint64_t cid;

    Push(todo_queue, task_ptr);
    cid = __sync_fetch_and_add(&coroutine_env.info.cid, 0);
    sem_post(&coroutine_env.manager_sem[ cid % MANAGER_CNT ]);
}

static int
do_disk_open(list_item_ptr(task_queue) task_ptr)
{
    struct open_arg_s *arg;

    arg = &task_ptr->args.open_arg;
    if (arg->flags & O_CREAT) // may refer to coroutine_set_disk_open()
        task_ptr->ret.val = open(arg->pathname, arg->flags, arg->mode);
    else
        task_ptr->ret.val = open(arg->pathname, arg->flags);

    if (task_ptr->ret.val >= 0)
        task_ptr->ret.err_code = 0;
    else
        task_ptr->ret.err_code = errno;
    task_ptr->action = act_disk_open_done;

    return 0;
}

static int
do_disk_read(list_item_ptr(task_queue) task_ptr)
{
    struct io_arg_s *arg;

    arg = &task_ptr->args.io_arg;
    task_ptr->ret.val = read(arg->fd, arg->buf, arg->count);
    if (task_ptr->ret.val >= 0)
        task_ptr->ret.err_code = 0;
    else
        task_ptr->ret.err_code = errno;
    task_ptr->action = act_disk_read_done;

    return 0;
}

static int
do_disk_write(list_item_ptr(task_queue) task_ptr)
{
    struct io_arg_s *arg;

    arg = &task_ptr->args.io_arg;
    task_ptr->ret.val = write(arg->fd, arg->buf, arg->count);
    if (task_ptr->ret.val >= 0)
        task_ptr->ret.err_code = 0;
    else
        task_ptr->ret.err_code = errno;
    task_ptr->action = act_disk_write_done;

    return 0;
}

static int
ev_sock_stop(list_item_ptr(task_queue) task_ptr, int retval, int err_code)
{
    ev_io_stop(CurrLoop, &task_ptr->ec.sock_watcher);
    if (task_ptr->args.io_arg.timeout_ms != 0)
        ev_timer_stop(CurrLoop, &task_ptr->ec.sock_timer);
    task_ptr->ret.val = retval;
    task_ptr->ret.err_code = err_code;
    task_ptr->action = act_sock_done;

    return 0;
}

static void
ev_sock_timeout(struct ev_loop *loop, ev_timer *timer_w, int event)
{
    list_item_ptr(task_queue) task_ptr;

    (void)loop;

    assert((event & EV_TIMEOUT) || (event & EV_ERROR));
    task_ptr = TIMER_WATCHER_REF_TASKPTR(timer_w);
    if (event & EV_ERROR)
    {
        /* in the case of EV_ERROR, is task_ptr valid? */
        ev_sock_stop(task_ptr, -1, ENODEV); /* use ENODEV to distinguish between possible errors and EV_ERROR */
        coroutine_notify_manager(task_ptr);
        return;
    }
    if (task_ptr->ec.have_io == 0)
        ev_sock_stop(task_ptr, -1, EWOULDBLOCK);
    else
        ev_sock_stop(task_ptr, task_ptr->ec.have_io, 0);
    coroutine_notify_manager(task_ptr);
}

static void
ev_sock_read(struct ev_loop *loop, ev_io *io_w, int event)
{
    list_item_ptr(task_queue) task_ptr;
    struct io_arg_s *arg;
    ssize_t io_bytes;
    char *buf;

    (void)loop;

    assert((event & EV_READ) || (event & EV_ERROR));
    task_ptr = IO_WATCHER_REF_TASKPTR(io_w);
    if (event & EV_ERROR)
    {
        ev_sock_stop(task_ptr, -1, ENODEV);
        coroutine_notify_manager(task_ptr);
        return;
    }

    arg = &task_ptr->args.io_arg;
    assert(fcntl(arg->fd, F_GETFL) & O_NONBLOCK);
    assert(task_ptr->action == act_sock_read);
    buf = arg->buf;

    if (task_ptr->ec.need_io == 0)
    {
        ev_sock_stop(task_ptr, task_ptr->ec.have_io, 0);
        coroutine_notify_manager(task_ptr);
        return;
    }

go:
    errno = 0;
    io_bytes = read(arg->fd, &buf[ task_ptr->ec.have_io ], task_ptr->ec.need_io);
    if (io_bytes == task_ptr->ec.need_io)
    {
        /* finished successfully */
        task_ptr->ec.have_io += io_bytes;
        task_ptr->ec.need_io -= io_bytes;
        ev_sock_stop(task_ptr, task_ptr->ec.have_io, 0);
    }
    else if (io_bytes > 0)
    {
        /* read successfully but haven't done yet */
        task_ptr->ec.need_io -= io_bytes;
        task_ptr->ec.have_io += io_bytes;
        goto go;
    }
    else if (io_bytes == 0)
    {
        /* remote peer closed connection */
        if ((errno == EAGAIN) || (errno == EWOULDBLOCK) || (errno == 0))
            ev_sock_stop(task_ptr, task_ptr->ec.have_io, errno);
        else
            ev_sock_stop(task_ptr, -1, errno);
    }
    else if (io_bytes == -1)
    {
        if (errno == EINTR) /* broke by signal */
            goto go;
        else if ((errno != EAGAIN) && (errno != EWOULDBLOCK)) /* unexpected failure */
            ev_sock_stop(task_ptr, io_bytes, errno);
        else /* read until EAGAIN / EWOULDBLOCK, need read later */
            return;
    }
    else /* unexpected return value, should never get here */
        abort();

    coroutine_notify_manager(task_ptr);
}

static int
do_sock_read(list_item_ptr(task_queue) task_ptr)
{
    struct io_arg_s *arg;

    arg = &task_ptr->args.io_arg;
    ev_io_init(&task_ptr->ec.sock_watcher, ev_sock_read, arg->fd, EV_READ);
    ev_io_start(CurrLoop, &task_ptr->ec.sock_watcher);
    if (arg->timeout_ms != 0)
    {
        ev_timer_init(&task_ptr->ec.sock_timer, ev_sock_timeout, arg->timeout_ms * 1.0 / 1000, 0.);
        ev_timer_start(CurrLoop, &task_ptr->ec.sock_timer);
    }
    task_ptr->ec.have_io = 0;
    task_ptr->ec.need_io = task_ptr->args.io_arg.count;

    return 0;
}

static void
ev_sock_write(struct ev_loop *loop, ev_io *io_w, int event)
{
    list_item_ptr(task_queue) task_ptr;
    struct io_arg_s *arg;
    ssize_t io_bytes;
    char *buf;

    (void)loop;

    assert((event & EV_WRITE) || (event & EV_ERROR));
    task_ptr = IO_WATCHER_REF_TASKPTR(io_w);
    if (event & EV_ERROR)
    {
        ev_sock_stop(task_ptr, -1, ENODEV);
        coroutine_notify_manager(task_ptr);
        return;
    }

    arg = &task_ptr->args.io_arg;
    assert(fcntl(arg->fd, F_GETFL) & O_NONBLOCK);
    assert(task_ptr->action == act_sock_write);
    buf = arg->buf;

    if (task_ptr->ec.need_io == 0)
    {
        ev_sock_stop(task_ptr, task_ptr->ec.have_io, 0);
        coroutine_notify_manager(task_ptr);
        return;
    }

go:
    errno = 0;
    io_bytes = write(arg->fd, &buf[ task_ptr->ec.have_io ], task_ptr->ec.need_io);
    if (io_bytes == task_ptr->ec.need_io)
    {
        /* write done */
        task_ptr->ec.have_io += io_bytes;
        task_ptr->ec.need_io -= io_bytes;
        ev_sock_stop(task_ptr, task_ptr->ec.have_io, 0);
    }
    else if (io_bytes > 0)
    {
        /* write successfully but haven't done yet */
        task_ptr->ec.need_io -= io_bytes;
        task_ptr->ec.have_io += io_bytes;
        goto go;
    }
    else if (io_bytes == 0)
    {
        /* remote peer closed connection */
        if ((errno == EAGAIN) || (errno == EWOULDBLOCK) || (errno == 0))
            ev_sock_stop(task_ptr, task_ptr->ec.have_io, errno);
        else
            ev_sock_stop(task_ptr, -1, errno);
    }
    else if (io_bytes == -1)
    {
        if (errno == EINTR) /* broke by signal */
            goto go;
        else if ((errno != EAGAIN) && (errno != EWOULDBLOCK)) /* unexpected failure */
            ev_sock_stop(task_ptr, io_bytes, errno);
        else /* write until EAGAIN / EWOULDBLOCK, need write later */
            return;
    }
    else /* unexpected return value, should never get here */
        abort();

    coroutine_notify_manager(task_ptr);
}

static int
do_sock_write(list_item_ptr(task_queue) task_ptr)
{
    struct io_arg_s *arg;

    arg = &task_ptr->args.io_arg;
    ev_io_init(&task_ptr->ec.sock_watcher, ev_sock_write, arg->fd, EV_WRITE);
    ev_io_start(CurrLoop, &task_ptr->ec.sock_watcher);
    if (arg->timeout_ms != 0)
    {
        ev_timer_init(&task_ptr->ec.sock_timer, ev_sock_timeout, arg->timeout_ms * 1.0 / 1000, 0.);
        ev_timer_start(CurrLoop, &task_ptr->ec.sock_timer);
    }
    task_ptr->ec.have_io = 0;
    task_ptr->ec.need_io = task_ptr->args.io_arg.count;

    return 0;
}

static int
do_tcp_blocked_connect(list_item_ptr(task_queue) task_ptr)
{
    struct connect_arg_s *arg;
    int ret;
    int sockfd;
    struct sockaddr_in server_addr;
    int flag;

    arg = &task_ptr->args.connect_arg;

    flag = 1;
    sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sockfd < 0)
    {
        task_ptr->ret.val = sockfd;
        task_ptr->ret.err_code = errno;
        task_ptr->action = act_tcp_blocked_connect_done;

        return 0;
    }
    ret = setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof flag);
    if (ret < 0)
    {
        close(sockfd);
        task_ptr->ret.val = ret;
        task_ptr->ret.err_code = errno;
        task_ptr->action = act_tcp_blocked_connect_done;

        return 0;
    }

    memset(&server_addr, 0, sizeof server_addr);
    server_addr.sin_addr.s_addr = arg->ip;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = arg->port;

    /* TODO
     * ret might be EINPROGRESS, try few times here
     */
    ret = connect(sockfd, (struct sockaddr*)&server_addr, sizeof server_addr);
    if (ret == 0)
    {
        task_ptr->ret.val = sockfd;
        task_ptr->ret.err_code = 0;
    }
    else
    {
        close(sockfd);
        task_ptr->ret.val = ret;
        task_ptr->ret.err_code = errno;
        task_ptr->action = act_tcp_blocked_connect_done;

        return 0;
    }

    crt_set_nonblock(sockfd);
    task_ptr->action = act_tcp_blocked_connect_done;

    return 0;
}

static int
ev_sock_connect_stop(struct ev_loop *loop, list_item_ptr(task_queue) task_ptr, int retval, int err_code)
{
    ev_io_stop(loop, &task_ptr->ec.sock_watcher);
    if (task_ptr->args.connect_arg.timeout_ms != 0)
        ev_timer_stop(loop, &task_ptr->ec.sock_timer);
    task_ptr->ret.val = retval;
    task_ptr->ret.err_code = err_code;
    task_ptr->action = act_tcp_timeout_connect_done;
    coroutine_notify_manager(task_ptr);

    return 0;
}

static void
ev_sock_connect_timeout(struct ev_loop *loop, ev_timer *timer_w, int event)
{
    list_item_ptr(task_queue) task_ptr;

    assert((event & EV_TIMEOUT) || (event & EV_ERROR));
    task_ptr = TIMER_WATCHER_REF_TASKPTR(timer_w);
    if (event & EV_ERROR)
    {
        ev_sock_connect_stop(loop, task_ptr, -1, ENODEV);
    }
    else
    {
        ev_sock_connect_stop(loop, task_ptr, -1, EWOULDBLOCK);
    }
}

static void
ev_sock_connect(struct ev_loop *loop, ev_io *io_w, int event)
{
    list_item_ptr(task_queue) task_ptr;
    struct connect_arg_s *arg;
    socklen_t len;
    int ret;

    ret = 0;
    len = sizeof ret;
    assert((event & EV_READ) || (event & EV_WRITE) || (event & EV_ERROR));
    task_ptr = IO_WATCHER_REF_TASKPTR(io_w);
    arg = &task_ptr->args.connect_arg;

    if (event & EV_ERROR)
    {
        close(arg->fd);
        ev_sock_connect_stop(loop, task_ptr, -1, ENODEV);
        return;
    }

    assert(task_ptr->action = act_tcp_timeout_connect);
    if (getsockopt(arg->fd, SOL_SOCKET, SO_ERROR, &ret, &len) < 0)
    {
        close(arg->fd);
        ev_sock_connect_stop(loop, task_ptr, -1, errno);
        return;
    }
    if (ret != 0)
    {
        close(arg->fd);
        ev_sock_connect_stop(loop, task_ptr, -1, errno);
        return;
    }

    /* connect successfully */
    ev_sock_connect_stop(loop, task_ptr, arg->fd, 0);
}

static int
do_tcp_timeout_connect(list_item_ptr(task_queue) task_ptr)
{
    struct connect_arg_s *arg;
    int ret;
    int sockfd;
    struct sockaddr_in server_addr;
    int flag;

    arg = &task_ptr->args.connect_arg;

    flag = 1;
    sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sockfd < 0)
    {
        task_ptr->ret.val = sockfd;
        task_ptr->ret.err_code = errno;
        task_ptr->action = act_tcp_timeout_connect_done;

        goto done;
    }
    ret = setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof flag);
    if (ret < 0)
    {
        close(sockfd);
        task_ptr->ret.val = ret;
        task_ptr->ret.err_code = errno;
        task_ptr->action = act_tcp_timeout_connect_done;

        goto done;
    }
    crt_set_nonblock(sockfd);

    memset(&server_addr, 0, sizeof server_addr);
    server_addr.sin_addr.s_addr = arg->ip;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = arg->port;

    ret = connect(sockfd, (struct sockaddr*)&server_addr, sizeof server_addr);
    if (ret < 0)
    {
        if (errno != EINPROGRESS)
        {
            close(sockfd);
            task_ptr->ret.val = ret;
            task_ptr->ret.err_code = errno;
            goto done;
        }
    }
    else if (ret == 0)
    {
        /* connected immediately */
        task_ptr->ret.val = sockfd;
        task_ptr->ret.err_code = 0;
        goto done;
    }
    else
    {
        /* should never reach here */
        abort();
    }

    /* connecting */
    arg->fd = sockfd;
    ev_io_init(&task_ptr->ec.sock_watcher, ev_sock_connect, arg->fd, EV_READ | EV_WRITE);
    ev_io_start(CurrLoop, &task_ptr->ec.sock_watcher);
    if (arg->timeout_ms != 0)
    {
        ev_timer_init(&task_ptr->ec.sock_timer, ev_sock_connect_timeout, arg->timeout_ms * 1.0 / 1000, 0.);
        ev_timer_start(CurrLoop, &task_ptr->ec.sock_timer);
    }
    goto leave;

done:
    task_ptr->action = act_tcp_timeout_connect_done;
    coroutine_notify_manager(task_ptr);

leave:
    return 0;
}

static void
timer_timeout(struct ev_loop *loop, ev_timer *timer_w, int event)
{
    list_item_ptr(task_queue) task_ptr;

    assert(event == EV_TIMEOUT);
    task_ptr = TIMER_WATCHER_REF_TASKPTR(timer_w);
    ev_timer_stop(loop, &task_ptr->ec.sock_timer);
    task_ptr->ret.val = 0;
    task_ptr->ret.err_code = 0;
    task_ptr->action = act_msleep_done;

    coroutine_notify_manager(task_ptr);
}

static int
do_msleep(list_item_ptr(task_queue) task_ptr)
{
    struct general_arg_s *arg;

    arg = &task_ptr->args.general_arg;
    ev_timer_init(&task_ptr->ec.sock_timer, timer_timeout, arg->u64 * 1.0 /  1000, 0.);
    ev_timer_start(CurrLoop, &task_ptr->ec.sock_timer);

    return 0;
}

static int
do_bg_run(list_item_ptr(task_queue) task_ptr)
{
    struct routine_arg_s *arg;
    int retval;

    arg = &task_ptr->args.routine_arg;
    retval = (arg->func)(arg->arg, arg->result);
    task_ptr->ret.val = retval;
    task_ptr->ret.err_code = 0;
    task_ptr->action = act_bg_run_done;

    return 0;
}

static void
ev_tcp_accept(struct ev_loop *loop, ev_io *io_w, int event)
{
    list_item_ptr(task_queue) task_ptr;
    struct tcp_accept_arg_s *arg;
    int accept_fd;

    (void)loop;

    assert((event & EV_READ) || (event & EV_ERROR));
    if (event & EV_ERROR)
    {
        accept_fd = -1;
        /* XXX
         * need to free task_ptr? so, can we fetch the correct task_ptr through
         * IO_WATCHER_REF_TASKPTR()?
         */
    }
    else
    {
        task_ptr = IO_WATCHER_REF_TASKPTR(io_w);
        arg = &task_ptr->args.tcp_accept_arg;
        assert(task_ptr->action == act_tcp_accept);
        accept_fd = accept(arg->sockfd, arg->addr, arg->addrlen);
        ev_io_stop(CurrLoop, &task_ptr->ec.sock_watcher);
    }

    task_ptr->action = act_tcp_accept_done;
    task_ptr->ret.val = accept_fd;  /* might be -1 */

    if (accept_fd < 0)
    {
        assert(accept_fd == -1);
        task_ptr->ret.err_code = errno;
    }
    else
    {
        crt_set_nonblock(accept_fd /* equal to task_ptr->ret.val */);
        task_ptr->ret.err_code = 0;
    }

    coroutine_notify_manager(task_ptr);
}

static int
do_tcp_accept(list_item_ptr(task_queue) task_ptr)
{
    struct tcp_accept_arg_s *arg;

    assert(task_ptr->action == act_tcp_accept);

    arg = &task_ptr->args.tcp_accept_arg;
    ev_io_init(&task_ptr->ec.sock_watcher, ev_tcp_accept, arg->sockfd, EV_READ);
    ev_io_start(CurrLoop, &task_ptr->ec.sock_watcher);

    return 0;
}

/**
 * @brief Do task in background worker thread
 *
 * @param task_ptr
 *
 * @return 0 if it's sync operation, 1 if async
 */
static int
do_task(list_item_ptr(task_queue) task_ptr)
{
    int retval;

    retval = -1; /* not necessary, to avoid compiler warning */

    switch (task_ptr->action)
    {
    case act_disk_open:
        do_disk_open(task_ptr);
        retval = 0;
        break;

    case act_disk_read:
        do_disk_read(task_ptr);
        retval = 0;
        break;

    case act_disk_write:
        do_disk_write(task_ptr);
        retval = 0;
        break;

    case act_sock_read:
        do_sock_read(task_ptr);
        retval = 1;
        break;

    case act_sock_write:
        do_sock_write(task_ptr);
        retval = 1;
        break;

    case act_tcp_blocked_connect:
        do_tcp_blocked_connect(task_ptr);
        retval = 0;
        break;

    case act_tcp_timeout_connect:
        do_tcp_timeout_connect(task_ptr);
        retval = 1;
        break;

    case act_msleep:
        do_msleep(task_ptr);
        retval = 1;
        break;

    case act_bg_run:
        do_bg_run(task_ptr);
        retval = 0;
        break;

    case act_tcp_accept:
        do_tcp_accept(task_ptr);
        retval = 1;
        break;

    default:
        abort();
        break;
    }

    return retval;
}

static void
new_event_handler(struct ev_loop *loop, ev_io *w, int event)
{
    ssize_t nread;
    unsigned char c;
    list_item_ptr(task_queue) task_ptr;
    int ret;

    (void)loop;
    (void)w;

    if (event == EV_READ)
    {
        nread = read(CurrReadPipe, &c, sizeof c);
        assert(nread == sizeof c);
        assert(c == 0xee);
        Shift(doing_queue, task_ptr);
        assert(task_ptr != NULL);

        ret = do_task(task_ptr);
        if (ret == 0)
        {
            coroutine_notify_manager(task_ptr);
        }
        else if (ret != 1)
        {
            abort();
        }
    }
    else
    {
        abort();
    }
}

static void *
new_background_worker(void *arg)
{
    g_thread_id = (intptr_t)arg;

    CurrLoop = ev_loop_new(EVBACKEND_EPOLL);
    assert(CurrLoop != NULL);

    ev_io_init(&CurrWatcher, new_event_handler, CurrReadPipe, EV_READ);
    ev_io_start(CurrLoop, &CurrWatcher);

    ev_run(CurrLoop, 0);

    return NULL;
}

/* }}} */
/* {{{ public functions but internal used only */

#define GenSetActionFunc(action_name)       \
    void coroutine_set_##action_name() {    \
        coroutine_env.curr_task_ptr[ g_thread_id ]->action = act_##action_name; \
    }
GenSetActionFunc(finished_coroutine);

int
coroutine_get_retval()
{
    return coroutine_env.curr_task_ptr[ g_thread_id ]->ret.val;
}

void
coroutine_set_disk_open(const char *pathname, int flags, ...)
{
    va_list ap;
    list_item_ptr(task_queue) task_ptr;

    task_ptr = coroutine_env.curr_task_ptr[ g_thread_id ];
    task_ptr->action = act_disk_open;
    task_ptr->args.open_arg.pathname = pathname;
    task_ptr->args.open_arg.flags = flags;
    /*
     * An interesting bug here, either you use `flags & O_CREAT' or
     * `flags | O_CREAT' will cause same result: the correct result,
     * even that `flags | O_CREAT' is apparently wrong here.
     * This is the same for the judgement statement in do_disk_open().
     */
    if (flags & O_CREAT)
    {
        va_start(ap, flags);
        task_ptr->args.open_arg.mode = va_arg(ap, mode_t);
        va_end(ap);
    }
    else
    {
        task_ptr->args.open_arg.mode = 0;
    }
}

void
coroutine_set_disk_read(int fd, void *buf, size_t count)
{
    list_item_ptr(task_queue) task_ptr;

    task_ptr = coroutine_env.curr_task_ptr[ g_thread_id ];
    task_ptr->action = act_disk_read;
    task_ptr->args.io_arg.fd    = fd;
    task_ptr->args.io_arg.buf   = buf;
    task_ptr->args.io_arg.count = count;
}

void
coroutine_set_disk_write(int fd, void *buf, size_t count)
{
    list_item_ptr(task_queue) task_ptr;

    task_ptr = coroutine_env.curr_task_ptr[ g_thread_id ];
    task_ptr->action = act_disk_write;
    task_ptr->args.io_arg.fd    = fd;
    task_ptr->args.io_arg.buf   = buf;
    task_ptr->args.io_arg.count = count;
}

void
coroutine_set_sock_read(int fd, void *buf, size_t count, int msec)
{
    list_item_ptr(task_queue) task_ptr;

    task_ptr = coroutine_env.curr_task_ptr[ g_thread_id ];
    task_ptr->action = act_sock_read;
    task_ptr->args.io_arg.fd    = fd;
    task_ptr->args.io_arg.buf   = buf;
    task_ptr->args.io_arg.count = count;
    task_ptr->args.io_arg.timeout_ms = msec;
}

void
coroutine_set_sock_write(int fd, void *buf, size_t count, int msec)
{
    list_item_ptr(task_queue) task_ptr;

    task_ptr = coroutine_env.curr_task_ptr[ g_thread_id ];
    task_ptr->action = act_sock_write;
    task_ptr->args.io_arg.fd    = fd;
    task_ptr->args.io_arg.buf   = buf;
    task_ptr->args.io_arg.count = count;
    task_ptr->args.io_arg.timeout_ms = msec;
}

void
coroutine_set_sock_connect(in_addr_t ip, in_port_t port, int msec)
{
    list_item_ptr(task_queue) task_ptr;

    task_ptr = coroutine_env.curr_task_ptr[ g_thread_id ];
    if (msec == -1)
        task_ptr->action = act_tcp_blocked_connect;
    else
        task_ptr->action = act_tcp_timeout_connect;
    task_ptr->args.connect_arg.ip = ip;
    task_ptr->args.connect_arg.port = port;
    task_ptr->args.connect_arg.timeout_ms = msec;
}

void
coroutine_notify_background_worker(void)
{
    unsigned char c;
    ssize_t nwrite;
    uint64_t bg_worker_id;

    assert(g_thread_id == 0);
    c = 0xee;
    Push(doing_queue, coroutine_env.curr_task_ptr[ g_thread_id ]);
    bg_worker_id = __sync_fetch_and_add(&coroutine_env.info.bg_worker_id, 1) % coroutine_env.background_worker_count;
    nwrite = write(coroutine_env.pipe_channel[bg_worker_id * 2 + 1], &c, sizeof c);
    assert(nwrite == sizeof c);
}

void
coroutine_get_context(ucontext_t **manager_context, ucontext_t **task_context)
{
    *manager_context = &coroutine_env.manager_context;
    *task_context = &(coroutine_env.curr_task_ptr[ g_thread_id ]->task_context);
}

/* }}} */
/* {{{ public interfaces */

int
init_coroutine_env(size_t worker_count)
{
    int ret;
    int flag;

    memset(&coroutine_env, 0, sizeof coroutine_env);

    if (worker_count == 0)
        coroutine_env.background_worker_count = BACKGROUND_WORKER_CNT;
    else
        coroutine_env.background_worker_count = worker_count;

    list_new(task_queue, todo_queue);
    coroutine_env.todo_queue = todo_queue;
    list_new(task_queue, doing_queue);
    coroutine_env.doing_queue = doing_queue;

    coroutine_env.background_worker_tid = acoro_malloc(coroutine_env.background_worker_count * sizeof(pthread_t));
    coroutine_env.pipe_channel = acoro_malloc(2 * coroutine_env.background_worker_count * sizeof(int));
    coroutine_env.worker_ev = acoro_malloc(coroutine_env.background_worker_count * sizeof(*coroutine_env.worker_ev));

    for (size_t i=0; i<MANAGER_CNT; i++)
    {
        sem_init(&coroutine_env.manager_sem[i], 0, 0);
        pthread_create(&coroutine_env.manager_tid[i], NULL, new_manager, (void*)(intptr_t)i);
        sched_yield();
    }
    for (size_t i=0; i<coroutine_env.background_worker_count; i++)
    {
        pipe(&coroutine_env.pipe_channel[i*2]);

        flag = fcntl(coroutine_env.pipe_channel[i*2], F_GETFL);
        assert(flag >= 0);
        ret = fcntl(coroutine_env.pipe_channel[i*2], F_SETFL, flag | O_NONBLOCK);
        assert(ret == 0);
        flag = fcntl(coroutine_env.pipe_channel[i*2+1], F_GETFL);
        assert(flag >= 0);
        ret = fcntl(coroutine_env.pipe_channel[i*2+1], F_SETFL, flag | O_NONBLOCK);
        assert(ret == 0);

        pthread_create(&coroutine_env.background_worker_tid[i], NULL, new_background_worker, (void*)(intptr_t)i);
        sched_yield();
    }

    return 0;
}

int
destroy_coroutine_env()
{
    list_destroy(coroutine_env.todo_queue);
    list_destroy(coroutine_env.doing_queue);
    for (int i=0; i<MANAGER_CNT; i++)
    {
        pthread_cancel(coroutine_env.manager_tid[i]);
        sem_destroy(&coroutine_env.manager_sem[i]);
        pthread_join(coroutine_env.manager_tid[i], NULL);
    }
    for (size_t i=0; i<coroutine_env.background_worker_count; i++)
    {
        pthread_cancel(coroutine_env.background_worker_tid[i]);
        pthread_join(coroutine_env.background_worker_tid[i], NULL);
    }
    for (size_t i=0; i<coroutine_env.background_worker_count*2; i++)
        close(coroutine_env.pipe_channel[i]);

    for (size_t i=0; i<coroutine_env.background_worker_count; i++)
    {
        ev_io_stop(coroutine_env.worker_ev[i].loop, &coroutine_env.worker_ev[i].watcher);
        ev_loop_destroy(coroutine_env.worker_ev[i].loop);
    }

    return 0;
}

int
crt_get_err_code()
{
    return coroutine_env.curr_task_ptr[ g_thread_id ]->ret.err_code;
}

int
crt_create(coroutine_t *cid, const void * __restrict attr __attribute__((unused)),
           launch_routine_t br, void * __restrict arg)
{
    coroutine_attr_t *attr_ptr;
    list_item_new(task_queue, task_ptr);

    if (cid == NULL)
        (void)__sync_add_and_fetch(&coroutine_env.info.cid, 1);
    else
        *cid = __sync_add_and_fetch(&coroutine_env.info.cid, 1);

    task_ptr->action = act_new_coroutine;
    task_ptr->args.init_arg.func = br;
    task_ptr->args.init_arg.func_arg = arg;
    attr_ptr = (coroutine_attr_t *)attr;
    if ((attr_ptr != NULL) && (attr_ptr->stacksize != 0))
    {
        task_ptr->args.init_arg.stack_size = attr_ptr->stacksize;
    }
    else
    {
        task_ptr->args.init_arg.stack_size = COROUTINE_STACK_SIZE;
    }

    Push(todo_queue, task_ptr);
    sem_post(&coroutine_env.manager_sem[0]); /* TODO post to one manager by round robin */

    return 0;
}

int
crt_attr_setstacksize(coroutine_attr_t *attr, size_t stacksize)
{
    attr->stacksize = stacksize;
    return 0;
}

int
crt_sched_yield(void)
{
    uint64_t cid;
    ucontext_t *manager_context, *task_context;

    coroutine_env.curr_task_ptr[ g_thread_id ]->action = act_sched_yield;
    Push(todo_queue, coroutine_env.curr_task_ptr[ g_thread_id ]);
    cid = __sync_fetch_and_add(&coroutine_env.info.cid, 0);
    sem_post(&coroutine_env.manager_sem[ cid % MANAGER_CNT ]);

    coroutine_get_context(&manager_context, &task_context);
    swapcontext(task_context, manager_context); // goto manager contenxt in manager thread

    return 0;
}

int
crt_set_nonblock(int fd)
{
    int flag;
    int ret = 0;

    if ((flag = fcntl(fd, F_GETFL)) < 0)
    {
        return errno;
    }
    if (fcntl(fd, F_SETFL, flag | O_NONBLOCK) < 0)
    {
        return errno;
    }

    return ret;
}

int
crt_set_block(int fd)
{
    int flag;
    int ret = 0;

    if ((flag = fcntl(fd, F_GETFL)) < 0)
    {
        return errno;
    }
    flag = (flag & (~O_NONBLOCK));
    if (fcntl(fd, F_SETFL, flag) < 0)
    {
        return errno;
    }

    return ret;
}

int
crt_msleep(uint64_t msec)
{
    int ret;
    list_item_ptr(task_queue) task_ptr;

    ret = -1;
    if (msec != 0)
    {
        task_ptr = coroutine_env.curr_task_ptr[ g_thread_id ];
        task_ptr->action = act_msleep;
        task_ptr->args.general_arg.u64 = msec;
        coroutine_notify_background_worker();

        ucontext_t *manager_context, *task_context;
        coroutine_get_context(&manager_context, &task_context);
        swapcontext(task_context, manager_context); // goto manager contenxt in manager thread

        ret = coroutine_get_retval();
    }

    return ret;
}

/**
 * @brief Run a routine in background thread without order restriction.
 *        NOTE that all crt_*() are *NOT* allowd in bg_routine
 */
int
crt_bg_run(bg_routine_t bg_routine, void *arg, void *result)
{
    int ret;
    list_item_ptr(task_queue) task_ptr;
    ucontext_t *manager_context, *task_context;

    task_ptr = coroutine_env.curr_task_ptr[ g_thread_id ];
    task_ptr->action = act_bg_run;
    task_ptr->args.routine_arg.func = bg_routine;
    task_ptr->args.routine_arg.arg = arg;
    task_ptr->args.routine_arg.result = result;
    coroutine_notify_background_worker();
    coroutine_get_context(&manager_context, &task_context);
    swapcontext(task_context, manager_context); // goto manager contenxt in manager thread

    ret = coroutine_get_retval();

    return ret;
}

/**
 * @brief A function corresponding to crt_tcp_accept() (release function:
 * close_accept_fd()) doesn't exists since we don't need that requirement
 * currently.
 *
 * @return
 */
int
crt_tcp_accept(int sockfd, struct sockaddr *addr, socklen_t *addrlen)
{
    int accept_fd;
    list_item_ptr(task_queue) task_ptr;
    ucontext_t *manager_context, *task_context;

    task_ptr = coroutine_env.curr_task_ptr[ g_thread_id ];
    task_ptr->action = act_tcp_accept;
    task_ptr->args.tcp_accept_arg.sockfd = sockfd;
    task_ptr->args.tcp_accept_arg.addr = addr;
    task_ptr->args.tcp_accept_arg.addrlen = addrlen;
    coroutine_notify_background_worker();
    coroutine_get_context(&manager_context, &task_context);
    swapcontext(task_context, manager_context); // goto manager contenxt in manager thread

    accept_fd = coroutine_get_retval();

    return accept_fd;
}

int
crt_tcp_prepare_sock(in_addr_t addr, uint16_t port)
{
    int sockfd;
    int flag;
    struct sockaddr_in server_addr;
    int ret;

    sockfd = -1;
    flag = 1;
    sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    ret = sockfd;

    if (sockfd < 0)
    {
        ret = CRT_ERR_SOCKET;
        goto exception;
    }
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof flag) != 0)
    {
        ret = CRT_ERR_SETSOCKOPT;
        goto exception;
    }
    bzero(&server_addr, sizeof server_addr);
    server_addr.sin_addr.s_addr = addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = port;
    if (bind(sockfd, (struct sockaddr *)&server_addr, sizeof server_addr) < 0)
    {
        ret = CRT_ERR_BIND;
        goto exception;
    }
    if (listen(sockfd, 128) < 0)
    {
        ret = CRT_ERR_LISTEN;
        goto exception;
    }
    if (crt_set_nonblock(sockfd) != 0)
    {
        ret = CRT_ERR_SET_NONBLOCK;
        goto exception;
    }

    goto leave;

exception:
    coroutine_env.curr_task_ptr[ g_thread_id ]->ret.err_code = errno;
    if (sockfd >= 0)
        close(sockfd);

leave:
    return ret;
}

int
crt_sem_init(crt_sem_t *sem, int pshared __attribute__((unused)), unsigned int value)
{
    sem->value = value;
    sem->task_ptr = NULL;

    sem->buf_capacity = 0;
    sem->buf_offset = 0;
    sem->buf = NULL;

    return 0;
}

int
crt_sem_post(crt_sem_t *sem)
{
    ++ sem->value;
    if (sem->task_ptr != NULL)
    {
        /* means: one coroutine is waiting in crt_sem_wait()
         *
         * if sem->task_ptr == NULL, means: no coroutine is waiting, in that
         * case, return directly
         */
        uint64_t cid;
        sem->task_ptr->action = act_sem_activation;
        Push(todo_queue, sem->task_ptr);
        cid = __sync_fetch_and_add(&coroutine_env.info.cid, 0);
        sem_post(&coroutine_env.manager_sem[ cid % MANAGER_CNT ]);
    }

    return 0;
}

/**
 * @brief This function should be ran in manager context.
 *        After calling this, the task_ptr is only stored in sem, both
 *        doing_queue and todo_queue have no record.
 *
 *        NOTE that ONLY one coroutine is allowed to wait one crt_sem_t at a
 *        given time, otherwise the behavior is undefined.
 */
int
crt_sem_wait(crt_sem_t *sem)
{
    ucontext_t *manager_context, *task_context;

    for (; ;)
    {
        if (sem->value > 0)
        {
            sem->task_ptr = NULL; /* we don't need switch back, so set it to NULL */
            -- sem->value;
            goto leave;
        }

        sem->task_ptr = coroutine_env.curr_task_ptr[ g_thread_id ];
        coroutine_get_context(&manager_context, &task_context);
        assert(&(sem->task_ptr->task_context) == task_context); /* check whether that coroutine who is waiting */
        swapcontext(task_context, manager_context); // goto manager contenxt in manager thread
    }

leave:
    return 0;
}

int
crt_sem_destroy(crt_sem_t *sem)
{
    memset(sem, 0xCC, sizeof *sem);
    return 0;
}

int
crt_sem_priority_post(crt_sem_t *sem, int flag)
{
    uint64_t cid;

    switch (flag)
    {
    case CRT_SEM_NORMAL_PRIORITY:
        crt_sem_post(sem);
        break;

    case CRT_SEM_HIGH_PRIORITY:
        abort();
        break;

    case CRT_SEM_CRITICAL_PRIORITY:
        ++ sem->value;
        /* no waiting coroutine exists, so we simply skip the following
         * procedure
         */
        if (sem->task_ptr != NULL)
        {
            sem->task_ptr->action = act_sem_activation;
            Unshift(todo_queue, sem->task_ptr);
            cid = __sync_fetch_and_add(&coroutine_env.info.cid, 0);
            sem_post(&coroutine_env.manager_sem[ cid % MANAGER_CNT ]);
        }
        break;

    default:
        abort();
        break;
    }

    return 0;
}

/* {{{ disk IO */

/* Actually, open(2) is a slow call, might be block while disk addressing or io
 * queue is full, etc. */
int
crt_disk_open(const char *pathname, int flags, ...)
{
    va_list ap;
    int retval;
    ucontext_t *manager_context, *task_context;
    mode_t mode;

    va_start(ap, flags);
    mode = va_arg(ap, mode_t);
    va_end(ap);

    coroutine_set_disk_open(pathname, flags, mode);
    coroutine_notify_background_worker();
    coroutine_get_context(&manager_context, &task_context);
    swapcontext(task_context, manager_context);
    retval = coroutine_get_retval();

    return retval;
}

/* }}} */

/* }}} */

/* vim: set expandtab tabstop=4 shiftwidth=4 foldmethod=marker: */
