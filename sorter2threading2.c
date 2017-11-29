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
#include "stack.c"

#define MAX_PATH_LENGTH 256

//the root is now an initial thread process
pid_t root;
pthread_t rootTID;
stack_safe *StackOfSortedFiles;
int numRows2;
int counterofthreads;
pthread_t* threadIds;
char *global_output_dir;
char *global_starting_dir;
char *global_column_to_sort;

int main (int argc, char* argv[]) {
	//check inputs 

	root = getpid();
	rootTID = pthread_self();
	printf("Initial PID: %d \n", root);

	char *errorMessage = "The program must specify a column to sort, given by the -c argument check documentation for a list of valid columns.\n";
	int i; 
	for (i = 0; i < argc; i++) { 
		//printf("%s\n", argv[i]); 
		char* argument = argv[i];
		if(strcmp(argument,"-d") == 0){
			global_starting_dir = argv[i+1];
		} else if(strcmp(argument,"-c") == 0) {
			global_column_to_sort = argv[i+1];
		} else if(strcmp(argument,"-o") == 0){
			global_output_dir = argv[i+1];
		}
	}

	if(global_column_to_sort == NULL) {
		printf("ERROR: No input column specified. Column to sort must be given with argument tag '-c'.\n");
		exit(0);
	}

	if(global_starting_dir == NULL) {
		printf("Staring directory not specified, defaulting to './' as the global_starting_dir.\n");
		global_starting_dir = ".";
	}

	if(global_output_dir == NULL) {
		printf("Output directory not specified, defaulting to './' as the global_output_dir.\n");
		global_output_dir = "."; //Will either be a valid global_starting_dir or ./ at this point in code
	}

	StackOfSortedFiles = stack_create(30);

	if(*global_column_to_sort==0){ 
		printf("%s",errorMessage);
		exit(1);
	} else {
		travdir(global_starting_dir, global_column_to_sort, global_output_dir);	
	}

	return 0;
}

//Helper function that sets the arguments for a thread that sorts a given file
args_sortFile * createThreadsSort(char* pathname, char* d_name, FILE* csvFile, char* output_dir, char* directory_path,int counter){
	args_sortFile *sortFileArgs = calloc(1, sizeof(args_sortFile));
	sortFileArgs->pathName = pathname;
	sortFileArgs->directoryName = d_name;
	sortFileArgs->csvFile = csvFile;
	sortFileArgs->directory_path = directory_path;
	sortFileArgs->output_dir = output_dir;
	sortFileArgs->counter = counter;
	return sortFileArgs;
}

args_travelDirectory * createThreadsTraverse(char * output_dir, int counter, pthread_t* threadHolder, DIR * directory, char *directory_path){
	args_travelDirectory* travelDirectoryArgs = (args_travelDirectory *)malloc(sizeof(args_travelDirectory));
	travelDirectoryArgs->output_dir = output_dir;
	travelDirectoryArgs->counter = counter;
	travelDirectoryArgs->threadHolder = threadHolder;
	travelDirectoryArgs->directory = directory;
	travelDirectoryArgs->directory_path = directory_path;

	return travelDirectoryArgs;
}

//Takes in a thread argument structure, and uses that to retrieve all necessary information.
void processFiletoSort(void* args){
	args_sortFile* sortFileArgs = args;
	sortFileArgs->counter++;
	
	threadIds[counterofthreads] = pthread_self(); 
	counterofthreads++;

	//change path for accessing the csv file
	//char * csvFileOutputPath = malloc(sizeof(char)*512);
	//Remove the ".csv" from d_name to insert "-sorted-VALIDCOLUMN.csv
	char *file_name = (char *) malloc(strlen(sortFileArgs->directoryName) + 1);
	strcpy(file_name, sortFileArgs->directoryName);					
	char *lastdot = strrchr(file_name, '.');
	if (lastdot != NULL) {
		*lastdot = '\0';
	}

	//sort the csv file
	//Push to the global stack of sorted files.
	//Row **rowSet;
	
	Row **tempRows = malloc(sizeof(struct Row*) * NUM_ROWS);
	sortnew(tempRows, sortFileArgs->csvFile, global_column_to_sort);
	
	push(StackOfSortedFiles, tempRows);

	free(file_name);

	pthread_exit(NULL);
}

//open the directory and create threadholder
int travdir(const char * input_dir_path, char* column_to_sort, const char * output_dir)
{
	numRows2 = 0;
	char *directory_path = (char *) malloc(MAX_PATH_LENGTH);
	strcpy(directory_path, input_dir_path);
	
	DIR * directory = opendir(directory_path);

	if (directory == NULL) {
        return 1;
	}

	//have one thread go through directories
	int numThreads = 50;
	counterofthreads = 0;
	pthread_t* threadHolder = (pthread_t*)(malloc(sizeof(pthread_t) * numThreads));
	threadIds = (pthread_t*)(malloc(sizeof(pthread_t) * numThreads));
	goThroughPath(createThreadsTraverse(output_dir, counterofthreads, threadHolder, directory, directory_path));

	free(directory_path);
	return 0;
}
	
//Function pointer to go through the directory path and finds csvs.
//Arguments are set without a helper are are set in this function
void goThroughPath(void* args){
	args_travelDirectory* travelDirectoryArgs = args;
	DIR* directory = travelDirectoryArgs->directory;
	char* directory_path = travelDirectoryArgs->directory_path;
	pthread_t* threadHolder = travelDirectoryArgs->threadHolder;
	char* finalDirectoryPath;
	char* output_dir = global_output_dir;
	threadIds[counterofthreads] = pthread_self();
	counterofthreads++;

	//while we go through the directory -> in the parent process keep looking for csv files
	while(directory != NULL) {
		struct dirent * currEntry;
		char * d_name;
		currEntry = readdir(directory);

		//making sure not to over thread bc memory
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

				if(*global_output_dir != 0){
					if(*d_name != 0){
						if(strcmp(d_name,global_output_dir)==0){
							finalDirectoryPath = (char *)calloc(1, strlen(directory_path) + strlen(global_output_dir) + sizeof("/AllFiles-sorted-") + sizeof(global_column_to_sort) + sizeof(".csv") + 3);
							strcpy(finalDirectoryPath,newDirectoryPath);
						}
					}
				}
				//We have found a new directory and must thus make a new thread for it.
				//This requires updating the counter of the parent directory, as well as adding the thread to the threadholder
				pthread_t thread;
				//counterofthreads++;
				pthread_create(&thread, 0, goThroughPath, createThreadsTraverse(output_dir, 0, threadHolder, newDirectory, newDirectoryPath));
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
			} else if(isAlreadySorted(pathname, global_column_to_sort)) {
				printf("File already sorted: %s\n", d_name);
				break;
			} else {
				csvFile = fopen(pathname, "r");
				if (csvFile != NULL) {
					//pathname has the full path to the file including extension
					//directory_path has only the parent directories of the file
					//instead of forking we call the method createThreadsSort
					pthread_t thread;
					pthread_create(&thread, 0, processFiletoSort, createThreadsSort(pathname, d_name, csvFile, output_dir, directory_path, travelDirectoryArgs->counter));
					travelDirectoryArgs->threadHolder[travelDirectoryArgs->counter++] = thread;
				}
			}	
		} else {
			printf("not a valid file or directory");
		}	
	}

	//WAIT FOR THREADS TO FINISH 
	int i=0,j=0;
	int rowSet1Length=0;
	int rowSet2Length=0;
	int totalthreads = travelDirectoryArgs->counter;

	//printf("The thread %u has a count of: %d\n", pthread_self(), totalthreads);

	//calling pthread_self() here only gives the thread id's of the thread for going through directories

	for(i = 0; i < travelDirectoryArgs->counter; i++){
		pthread_join(travelDirectoryArgs->threadHolder[i], NULL);
	}

	free(travelDirectoryArgs->directory);

	//Anything that occurs in this conditional will only be done by the root thread
	//TODO: Parallelize poping from the stack
	if(getpid() == root && pthread_self() == rootTID){

		printf("TIDS of all child threads: ");
		for(i = 0; i < counterofthreads; i++){
			printf("%u,", threadIds[i]);
		}

		free(threadIds);
		printf("\n");
		printf("Total number of threads: %d\n\r", counterofthreads);
		
		//In this implementation we will pop and push from the stack once the threads have all completed
		//While the stack is not empty, we pop twice and merge.
		//If one of the Row ** that we pop is empty then we must have all of the rows merged together and can exit
		printf("\n");

		Row ** sortedRows;

		while(!is_empty(StackOfSortedFiles)) {
			//printf("The stack has %d elements in it.\n", StackOfSortedFiles->count);
			if(StackOfSortedFiles->count > 1) {

				Row ** rowSet1 = pop(StackOfSortedFiles);
				Row ** rowSet2 = pop(StackOfSortedFiles);
				rowSet1Length = getAmountOfRows(rowSet1);
				rowSet2Length = getAmountOfRows(rowSet2);
				sortedRows = malloc(sizeof(Row) * (rowSet1Length + rowSet2Length + 1));

				if(sortedRows != NULL) {

					mergeRowsTwoFinger(sortedRows, rowSet1, rowSet2);
					push(StackOfSortedFiles, sortedRows);

				} else {

				}
			} else if (StackOfSortedFiles->count == 1) {

				sortedRows = pop(StackOfSortedFiles);
				rowSet1Length = getAmountOfRows(sortedRows);

			}
		}
		printf("The stack is now empty. The rows are now stored in sortedRows.\n");

		printf("The rows will now be printed to the masterCSV file.\n");

		//open the output directory again and put the csv file there.
		if(*global_output_dir=='.'){
			finalDirectoryPath = (char *)calloc(1, strlen(global_output_dir) + sizeof("/AllFiles-sorted-") + sizeof(global_column_to_sort) + sizeof(".csv") + 5);
			strcat(finalDirectoryPath, global_output_dir);
		}

		if(finalDirectoryPath[strlen(finalDirectoryPath) - 1] == '/') {
			strcat(finalDirectoryPath,"AllFiles-sorted-");
		} else {
			strcat(finalDirectoryPath,"/AllFiles-sorted-");
		}

		strcat(finalDirectoryPath, global_column_to_sort);
		strcat(finalDirectoryPath,".csv");

		FILE *csvFileOut = fopen(finalDirectoryPath,"w");

		printToCSV(csvFileOut, sortedRows, rowSet1Length, NUM_COLS);

		stack_destroy(StackOfSortedFiles);

		//free the rows poped from the stack
		for(i = 0; i < rowSet1Length; i++) {
			//for(j = 0; j < NUM_COLS; j++) {
				//free(sortedRows[i]->colEntries[j].value);
				//sortedRows[i]->colEntries[j].value = NULL;
				//free(sortedRows[i]->colEntries[j].type);
				//sortedRows[i]->colEntries[j].type = NULL;
			//}
			free(sortedRows[i]);
			sortedRows[i] = NULL;
		}

		free(sortedRows);
		sortedRows = NULL;
		
		free(finalDirectoryPath);
		free(args);
		exit(0);
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
