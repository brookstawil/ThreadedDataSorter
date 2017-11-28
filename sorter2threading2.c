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

	if(global_starting_dir == NULL) {
		printf("Staring directory not specified, defaulting to './' as the global_starting_dir.\n");
		global_starting_dir = "./";
	}

	if(global_output_dir == NULL) {
		printf("Output directory not specified, defaulting to './' as the global_output_dir.\n");
		global_output_dir = "./"; //Will either be a valid global_starting_dir or ./ at this point in code
	}

	/*if(output_dir == NULL){
		if(global_starting_dir == NULL) {
			output_dir = "./";
		} else {
			output_dir = (char *)malloc(strlen(statring_dir) + 1);
			strcpy(output_dir, global_starting_dir);
		}
		if(strcmp(d_name,output_dir)==0){
			finalDirectoryPath = (char *)malloc(strlen(directory_path) + strlen(d_name) + 2);
			strcpy(finalDirectoryPath,newDirectoryPath);
			printf("the final directory path is %s \n", finalDirectoryPath);
		}
	} else { //Default behavior dictates that if no output path is given the current directory, either -d argument or ./ if no -d is given
		printf("Output directory is not null.\n");
	}*/

	StackOfSortedFiles = stack_create(30);
	//printf("Just after creation, is the stack empty? : %d\n", is_empty(StackOfSortedFiles));

	//TODO: Move the code that determines the output_dir to over here

	if(*global_column_to_sort==0){ 
		printf("%s",errorMessage);
		exit(1);
	} else {
		//printf("all possible exist %s, %s, %s\n", column_to_sort, global_starting_dir, output_dir);
		travdir(global_starting_dir, global_column_to_sort, global_output_dir);	
	}

	return 0;
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
	travelDirectoryArgs->column_to_sort = (char *) malloc(strlen(column_to_sort) + 1);
	strcpy(travelDirectoryArgs->column_to_sort, column_to_sort);

	return travelDirectoryArgs;
}

args_sortedRowStackPop * createStackPop(Row ** row1, Row ** row2) {

}

//Takes in a thread argument structure, and uses that to retrieve all necessary information.
void processFiletoSort(void* args){
	args_sortFile* sortFileArgs = args;
	sortFileArgs->counter++;
	
	printf("Thread %u is sorting the file: %s \n", pthread_self(),sortFileArgs->directoryName);
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
	
	//TODO: Move this output file determination to before the threading even begins
	//Default behavior dumps files into input directory
	/*if(sortFileArgs->output_dir == NULL) {
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

	*/
	//sort the csv file
	//Push to the global stack of sorted files.
	push(StackOfSortedFiles, sortnew(sortFileArgs->csvFile, global_column_to_sort));
	numRows2 += getNumberofRows();
	//printf("the total number of rows thus far are %d \n", numRows2);

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

	//initialize this to hold 20 threads for now
	//have one thread go through directories
	int numThreads = 40;
	counterofthreads = 0;
	pthread_t* threadHolder = (pthread_t*)(malloc(sizeof(pthread_t) * numThreads));
	threadIds = (pthread_t*)(malloc(sizeof(pthread_t) * numThreads));
	goThroughPath(createThreadsTraverse(output_dir, 0, threadHolder, directory, directory_path, column_to_sort));

	free(directory_path);
	return 0;
}
	
//Function pointer to go through the directory path and finds csvs.
//Arguments are set without a helper are are set in this function
void goThroughPath(void* args){
	args_travelDirectory* travelDirectoryArgs = args;
	printf("This is a traversing thread with a TID: %u \n", pthread_self());
	//printf("This thread is searching though: %s\n", travelDirectoryArgs->directory_path);
	//copy from struct to this function
	char* column_to_sort = travelDirectoryArgs->column_to_sort;
	//printf("The column to sort is %s \n", column_to_sort);
	DIR* directory = travelDirectoryArgs->directory;
	char* directory_path = travelDirectoryArgs->directory_path;
	char* output_dir = global_output_dir;
	pthread_t* threadHolder = travelDirectoryArgs->threadHolder;
	char* finalDirectoryPath;

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

				//We have found a new directory and must thus make a new thread for it.
				//This requires updating the counter of the parent directory, as well as adding the thread to the threadholder
				pthread_t thread;
				counterofthreads++;
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
				break;
			} else {
				csvFile = fopen(pathname, "r");
				if (csvFile != NULL) {
					//pathname has the full path to the file including extension
					//directory_path has only the parent directories of the file
					//instead of forking we call the method createThreadsSort
					pthread_t thread;
					pthread_create(&thread, 0, processFiletoSort, createThreadsSort(pathname, d_name, column_to_sort, csvFile, output_dir, directory_path, travelDirectoryArgs->counter));
					travelDirectoryArgs->threadHolder[travelDirectoryArgs->counter++] = thread;
				}
			}	
		} else {
			printf("not a valid file or directory");
		}	
	}

	//WAIT FOR THREADS TO FINISH 
	int i,j;
	int row1Length,row2Length;
	int totalthreads = travelDirectoryArgs->counter;

	threadIds[counterofthreads] = pthread_self(); 
	printf("The thread %u has a count of: %d\n", pthread_self(), totalthreads);

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
				Row ** row1 = pop(StackOfSortedFiles);
				Row ** row2 = pop(StackOfSortedFiles);
				//printf("Popping row1 from the stack, first movie has director_name: %s\n", row1[0]->colEntries[1].value);
				//printf("Popping row2 from the stack, first movie has director_name: %s\n", row2[0]->colEntries[1].value);
				sortedRows = mergeRowsTwoFinger(row1, row2, &row1Length, &row2Length);

				printf("Row1 has length: %d\n",row1Length);
				printf("Row2 has length: %d\n",row2Length);

				//free the rows poped from the stack
				/*for(i = 0; i < row1Length; i++) {
					for(j = 0; j < NUM_COLS; j++) {
						free(row1[i]->colEntries[j].value);
						row1[i]->colEntries[j].value = NULL;
						free(row1[i]->colEntries[j].type);
						row1[i]->colEntries[j].type = NULL;
					}
					free(row1[i]);
					row1[i] = NULL;
				}

				for(i = 0; i < row2Length; i++) {
					for(j = 0; j < NUM_COLS; j++) {
						free(row2[i]->colEntries[j].value);
						row2[i]->colEntries[j].value = NULL;
						free(row2[i]->colEntries[j].type);
						row2[i]->colEntries[j].type = NULL;
					}
					free(row2[i]);
					row2[i] = NULL;
				}

				free(row1);
				row1 = NULL;
				free(row2);
				row2 = NULL;*/

				//printf("Row1 and Row2 have been merged. The first director is: %s\tThe director is: %s\n", sortedRows[0]->colEntries[1].value,sortedRows[1]->colEntries[1].value);
				push(StackOfSortedFiles, sortedRows);
			} else if (StackOfSortedFiles->count == 1) {
				sortedRows = pop(StackOfSortedFiles);
				//printf("Popping row1 from the stack, first movie has director_name: %s\n", sortedRows[0]->colEntries[1].value);
			}
		}
		printf("The stack is now empty. The rows are now stored in sortedRows.\n");

		printf("The rows will now be printed to the masterCSV file.\n");

		//char * csvFileOutputPath = malloc(sizeof(char)*512);
		
		//TODO: Move this output file determination to before the threading even begins
		//Default behavior dumps files into input directory
		finalDirectoryPath = (char *)calloc(1, strlen(directory_path) + sizeof("/AllFiles-sorted-") + sizeof(global_column_to_sort) + sizeof(".csv") + 1);
		
		printf("The directory path is currently :'%s'\n",finalDirectoryPath);

		strcpy(finalDirectoryPath, directory_path);

		printf("The directory path is currently :'%s'\n",finalDirectoryPath);
		
		/*if(output_dir == NULL) {
			
		} //else { //If given an output directory
			//DIR * outputDirectoryPath = opendir(finalDirectoryPath);
			struct stat sb;
			if (stat(output_dir, &sb) == -1) {
				mkdir(output_dir, 0700); //RWX for owner
			} 
			
			//strcpy(csvFileOutputPath,output_dir);
		//}*/
		//open the output directory again and put the csv file there.
		strcat(finalDirectoryPath,"/AllFiles-sorted-");
		
		printf("The column_to_sort is: %s\n", column_to_sort);
		printf("The directory path is currently :'%s'\n",finalDirectoryPath);

		strcat(finalDirectoryPath, column_to_sort);

		printf("The directory path is currently :'%s'\n",finalDirectoryPath);


		strcat(finalDirectoryPath,".csv");

		printf("The file being written to is: %s\n",finalDirectoryPath);

		
		printf("The last character in the string is: '%d'\n",finalDirectoryPath[strlen(finalDirectoryPath)]);

		FILE *csvFileOut = fopen(finalDirectoryPath,"w");
		
		printToCSV(csvFileOut, sortedRows, numRows2, NUM_COLS);

		stack_destroy(StackOfSortedFiles);

		//free the rows poped from the stack
		/*for(i = 0; i < row1Length; i++) {
			for(j = 0; j < NUM_COLS; j++) {
				free(row1[i]->colEntries[j].value);
				row1[i]->colEntries[j].value = NULL;
				free(row1[i]->colEntries[j].type);
				row1[i]->colEntries[j].type = NULL;
			}
			free(row1[i]);
			row1[i] = NULL;
		}

		for(i = 0; i < row2Length; i++) {
			for(j = 0; j < NUM_COLS; j++) {
				free(row2[i]->colEntries[j].value);
				row2[i]->colEntries[j].value = NULL;
				free(row2[i]->colEntries[j].type);
				row2[i]->colEntries[j].type = NULL;
			}
			free(row2[i]);
			row2[i] = NULL;
		}

		free(row1);
		row1 = NULL;
		free(row2);
		row2 = NULL;*/
		
		free(sortedRows);
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
