#ifndef _UNLOAD_H
#define _UNLOAD_H

#define DEBUG 1
#define LOG(si,format,args...) \
	if (DEBUG) { \
		pid_t pid = getpid(); \
		pid_t tid = syscall(SYS_gettid); \
		(si).log("p=%5u t=%5u f=%s " format, pid, tid, __FUNCTION__, ##args); \
	}

#define MUTEX(m)   pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
#define LOCKM(m)   pthread_mutex_lock(&m)
#define UNLOCKM(m) pthread_mutex_unlock(&m)

#endif
