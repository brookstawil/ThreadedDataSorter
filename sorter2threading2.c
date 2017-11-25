#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <errno.h>
#include <dirent.h>
#include <pthread.h>
#include <sys/types.h>
#include "sorterthreading.c"
#include "sorter2.h"

#define MAX_PATH_LENGTH 256

//the root is now an initial thread process
pid_t root;

int main (int argc, char* argv[]) {
	//check inputs 
	char* column_to_sort="";
	char* starting_dir="";
	char* output_dir="";

	root = getpid();
	printf("Initial PID: %d \n", root);

	char *errorMessage = "The command line must specify a column to sort with arg -c \nfor example:\n./sorter -c  valid_column -d inputdir -o outputdir\n./sorter -c  valid_column -d inputdir\n./sorter -c  valid_column\n";
	int i; 
	for (i = 0; i < argc; i++) { 
		//printf("%s\n", argv[i]); 
		char* argument = argv[i];
		if(strcmp(argument,"-d") == 0){
			starting_dir = argv[i+1];
		} else if(strcmp(argument,"-c") == 0) {
			column_to_sort = argv[i+1];
		} else if(strcmp(argument,"-o") == 0){
			output_dir = argv[i+1];
		}
	}

	if(*column_to_sort==0){ 
		printf("%s",errorMessage);
		exit(1);
	} else if (*starting_dir==0){
		//check for an output directory
		if(*output_dir==0){
			//time to check if this is a csv file or a directory
			travdir("./", column_to_sort, NULL);
		}
		else{
			//there is a valid -c and valid -o parameter 
			travdir("./", column_to_sort, output_dir);
		}
	} else if (*output_dir==0){ //Default behavior is search the current directory - if output_dir is null
		//assume that starting dir is not null
		travdir(starting_dir, column_to_sort, NULL);
	} else {
		//printf("all possible exist %s, %s, %s\n", column_to_sort, starting_dir, output_dir);
		travdir(starting_dir, column_to_sort, output_dir);	
	}

	return 0;
}

//Takes in a thread argument structure, and uses that to retrieve all necessary information.
void processFiletoSort(void* args){
	args_sortFile* sortFileArgs = args;
	//THOUGHTS: MAYBE OPEN THE CSVFILE HERE AND NOT IN TRAVDIR ????
	printf("Thread %u is sorting the file: %s \n", pthread_self() ,sortFileArgs->directoryName);


	//change path for accessing the csv file
	char * csvFileOutputPath = malloc(sizeof(char)*512);
	//Remove the ".csv" from d_name to insert "-sorted-VALIDCOLUMN.csv
	char *file_name = (char *) malloc(strlen(sortFileArgs->directoryName) + 1);
	strcpy(file_name, sortFileArgs->directoryName);					
	char *lastdot = strrchr(file_name, '.');
	if (lastdot != NULL) {
		*lastdot = '\0';
	}
					
	//Default behavior dumps files into input directory
	if(sortFileArgs->output_dir == NULL) {
		strcpy(csvFileOutputPath,sortFileArgs->directory_path);
	} else { //If given an output directory
		struct stat sb;
		if (stat(sortFileArgs->output_dir, &sb) == -1) {
			mkdir(sortFileArgs->output_dir, 0700); //RWX for owner
		} 
		strcpy(csvFileOutputPath,sortFileArgs->output_dir);
	}

	strcat(csvFileOutputPath,"/");
	strcat(csvFileOutputPath,file_name);
	strcat(csvFileOutputPath,"-sorted-");
	strcat(csvFileOutputPath,sortFileArgs->column_to_sort);
	strcat(csvFileOutputPath,".csv");

	FILE *csvFileOut = fopen(csvFileOutputPath,"w");

	//sort the csv file
	char* column_to_sort="";
	column_to_sort=sortFileArgs->column_to_sort;
	sortnew(sortFileArgs->csvFile, csvFileOut, column_to_sort);
	//free(csvFileOutputPath);
	//free(file_name);

	pthread_exit(NULL);
}

//Helper function that sets the arguments for a thread that sorts a given file
args_sortFile * createThreadsSort(char* pathname, char* d_name, char* column_to_sort, FILE* csvFile, char* output_dir, char* directory_path,int counter){
	args_sortFile *sortFileArgs = calloc(1, sizeof(args_sortFile));
	sortFileArgs->pathName = pathname;
	sortFileArgs->directoryName = d_name;
	sortFileArgs->csvFile = csvFile;
	sortFileArgs->column_to_sort=column_to_sort;
	sortFileArgs->directory_path = directory_path;
	sortFileArgs->output_dir = output_dir;
	sortFileArgs->counter = counter;
	return sortFileArgs;
}

args_travelDirectory * createThreadsTraverse(char * output_dir, int counter, pthread_t* threadHolder, DIR * directory, char *directory_path, char* column_to_sort){
	
	args_travelDirectory* travelDirectoryArgs = (args_travelDirectory *)malloc(sizeof(args_travelDirectory));

	travelDirectoryArgs->output_dir = output_dir;
	travelDirectoryArgs->counter = counter;
	travelDirectoryArgs->threadHolder = threadHolder;
	travelDirectoryArgs->directory = directory;
	travelDirectoryArgs->directory_path = directory_path;
	travelDirectoryArgs->column_to_sort = (char*)(calloc(1,strlen(column_to_sort)));
	strcpy(travelDirectoryArgs->column_to_sort,"movie_title");
	//printf("The column to sort in travelDirectoryArgs is %s \n", travelDirectoryArgs->column_to_sort);

	return travelDirectoryArgs;
}

//open the directory and create threadholder
int travdir(const char * input_dir_path, char* column_to_sort, const char * output_dir)
{
	char *directory_path = (char *) malloc(MAX_PATH_LENGTH);
	strcpy(directory_path, input_dir_path);
	
	DIR * directory = opendir(directory_path);

	if (directory == NULL) {
        return 1;
	}

	//initialize this to hold 20 threads for now
	//have one thread go through directories
	int numThreads = 20;
	pthread_t* threadHolder = (pthread_t*)(malloc(sizeof(pthread_t) * numThreads));
	
	goThroughPath(createThreadsTraverse(output_dir, 0, threadHolder, directory, directory_path, column_to_sort));

	return 0;
}
	
//Function pointer to go through the directory path and finds csvs.
//Arguments are set without a helper are are set in this function
void goThroughPath(void* args){
	args_travelDirectory* travelDirectoryArgs = args;

	printf("This is a traversing thread with a TID: %u \n", pthread_self());
	printf("This thread is searching though: %s\n", travelDirectoryArgs->directory_path);
	//copy from struct to this function
	char* column_to_sort = travelDirectoryArgs->column_to_sort;
	//printf("The column to sort is %s \n", column_to_sort);
	DIR* directory = travelDirectoryArgs->directory;
	char* directory_path = travelDirectoryArgs->directory_path;
	char* output_dir = travelDirectoryArgs->output_dir;

	//while we go through the directory -> in the parent process keep looking for csv files
	while(directory != NULL) {
		struct dirent * currEntry;
		char * d_name;
		currEntry = readdir(directory);

		//making sure not to fork bomb
		if(travelDirectoryArgs->counter == 256){
			break;
		}
		//end of file stream, break-> now wait for children and children's children
		if(!currEntry) {
			break;
		}
		//d_name is the current directory/file
		d_name = currEntry->d_name;

		//this is a directory 
		if(currEntry->d_type==DT_DIR) {
			if(strcmp(d_name,".") != 0 && strcmp(d_name, "..") != 0) {
				//need to EXTEND THE PATH for next travdir call, working dir doesn't change (think adir/ -> adir/bdir/....)
				int pathlength = 0;	
				char path[MAX_PATH_LENGTH];
				pathlength = snprintf(path, MAX_PATH_LENGTH, "%s/%s",currEntry, d_name);
				if(pathlength > MAX_PATH_LENGTH-1) {
					printf("ERROR: Path length is too long");
					return;
				}
				char * newDirectoryPath = (char *)malloc(strlen(directory_path) + strlen(d_name) + 2);
				strcpy(newDirectoryPath, directory_path);

				//open new directory again
				strcat(newDirectoryPath,"/");
				strcat(newDirectoryPath,d_name);
				DIR * newDirectory = opendir(newDirectoryPath);

				//We have found a new directory and must thus make a new thread for it.
				//This requires updating the counter of the parent directory, as well as adding the thread to the threadholder

				//We can just call travdir again and a new thread with proper args will be created
				int numThreads = 20;
				pthread_t* threadHolder = (pthread_t*)(malloc(sizeof(pthread_t) * numThreads));
				
				pthread_t thread;
				pthread_create(&thread, 0, goThroughPath, createThreadsTraverse(output_dir, 0, threadHolder, newDirectory, newDirectoryPath, column_to_sort));

				travelDirectoryArgs->threadHolder[travelDirectoryArgs->counter++] = thread;
			}
		} 
		else if(currEntry->d_type == DT_REG) { 	//regular files, need to check to ensure ".csv"
			//need a for loop to go through the directory to get all csvs - pointer and travdir once at the end of the list of csvs in that one dir	
			char pathname [256];
			FILE* csvFile;
			sprintf(pathname, "%s/%s", directory_path, d_name);

			//Check to see if the file is a csv
			char *lastdot = strrchr(d_name, '.');

			if (strcmp(lastdot,".csv") != 0) {
				printf("File is not a .csv: %s\n", d_name);
			} else if(isAlreadySorted(pathname, column_to_sort)) {
				printf("File already sorted: %s\n", d_name);
			} else {
				csvFile = fopen(pathname, "r");
				if (csvFile != NULL) {
					//pathname has the full path to the file including extension
					//directory_path has only the parent directories of the file
					//instead of forking we call the method createThreadsSort
					pthread_t thread;
					pthread_create(&thread, 0, processFiletoSort, createThreadsSort(pathname, d_name, column_to_sort, csvFile, output_dir, directory_path, 0));
					travelDirectoryArgs->threadHolder[travelDirectoryArgs->counter++] = thread;
				}
			}	
		} else {

		}	
	}

	//WAIT FOR THREADS TO FINISH 
	int i,j;
	int totalthreads = travelDirectoryArgs->counter;

	printf("The thread %u has a count of: %d\n", pthread_self(), totalthreads);

	printf("I am thread %u and these are my child threads: ", pthread_self());
	for(i = 0; i < travelDirectoryArgs->counter; i++){
		printf("%u, ",travelDirectoryArgs->threadHolder[i]);
	}
	printf("\n");

	for(i = 0; i < travelDirectoryArgs->counter; i++){
		pthread_join(travelDirectoryArgs->threadHolder[i], NULL);
	}
	
	//while(totalthreads > 0){
		//call pthread_join() to wait for other threads to terminate
		//printf("TIDS of all child threads%d ", pthread_self());
		//need to print out the thread ids somehow here too
		//pthread_join(thread, NULL);
		//--totalthreads;
	//}


	if(getpid() == root){
		printf("\nTotal number of threads: %d\n\r", travelDirectoryArgs->counter);	
	}

	pthread_exit(NULL);
}

//If it already contains the phrase -sorted-SOMEVALIDCOLUMN.csv then the file is already sorted
//Returns 0 if the file has not yet been sorted
//Returns 1 if the file has been sorted
int isAlreadySorted(char *pathname,char *column_to_sort) {
	char *compareString = (char *) malloc(strlen("-sorted-") + strlen(column_to_sort) + strlen(".csv") + 1);
	//build the -sorted-SOMEVALIDCOLUMN.csv string
	strcpy(compareString, "-sorted-");
	strcat(compareString, column_to_sort);
	strcat(compareString, ".csv");
	if(strstr(pathname,compareString) == NULL) {
		free(compareString);		
		return 0;
	} else {
		free(compareString);
		return 1;		
	}
}
