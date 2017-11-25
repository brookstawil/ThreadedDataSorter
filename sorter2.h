#include <unistd.h>
#include <pthread.h>

struct stat {
    dev_t     st_dev;     /* ID of device containing file */
    ino_t     st_ino;     /* inode number */
    mode_t    st_mode;    /* protection */
    nlink_t   st_nlink;   /* number of hard links */
    uid_t     st_uid;     /* user ID of owner */
    gid_t     st_gid;     /* group ID of owner */
    dev_t     st_rdev;    /* device ID (if special file) */
    off_t     st_size;    /* total size, in bytes */
    blksize_t st_blksize; /* blocksize for file system I/O */
    blkcnt_t  st_blocks;  /* number of 512B blocks allocated */
    time_t    st_atime;   /* time of last access */
    time_t    st_mtime;   /* time of last modification */
    time_t    st_ctime;   /* time of last status change */
};

int travdir(const char * input_dir_path, char* column_to_sort, const char * output_dir);
void processFiletoSort(void* margs);
void goThroughPath(void* margs2);
void createThreadsSort(char* pathname, char* d_name, char* column_to_sort, FILE* csvFile, char* output_dir, char* directory_path, int counter);
int isAlreadySorted(char *pathname,char *column_to_sort);