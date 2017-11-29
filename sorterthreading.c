#include <stdio.h>
#include <string.h>
#include "sorter.h"
#include "mergesort.c"

#define NUM_COLS 28 //Code works for 28 columns
#define NUM_ROWS 8192 //Max number of rows in a file is 8192
#define HEADER_LINE "color,director_name,num_critic_for_reviews,duration,director_facebook_likes,actor_3_facebook_likes,actor_2_name,actor_1_facebook_likes,gross,genres,actor_1_name,movie_title,num_voted_users,cast_total_facebook_likes,actor_3_name,facenumber_in_poster,plot_keywords,movie_imdb_link,num_user_for_reviews,language,country,content_rating,budget,title_year,actor_2_facebook_likes,imdb_score,aspect_ratio,movie_facebook_likes\n"
#define DELIMITERS ",\r\n"

const char* validColumns[] = {
    "color",
    "director_name",
    "num_critic_for_reviews",
    "duration",
    "director_facebook_likes",
    "actor_3_facebook_likes",
    "actor_2_name",
    "actor_1_facebook_likes",
    "gross",
    "genres",
    "actor_1_name",
    "movie_title",
    "num_voted_users",
    "cast_total_facebook_likes",
    "actor_3_name",
    "facenumber_in_poster",
    "plot_keywords",
    "movie_imdb_link",
    "num_user_for_reviews",
    "language",
    "country",
    "content_rating",
    "budget",
    "title_year",
    "actor_2_facebook_likes",
    "imdb_score",
    "aspect_ratio",
    "movie_facebook_likes"
};

const char* validColumnTypes[] = {
    "char",
    "char",
    "int",
    "int",
    "int",
    "int",
    "char",
    "int",
    "int",
    "char",
    "char",
    "char",
    "int",
    "int",
    "char",
    "int",
    "char",
    "char",
    "int",
    "char",
    "char",
    "char",
    "int",
    "int",
    "int",
    "float",
    "float",
    "int"
};

int rowCountforFile;

void sortnew(Row ** rowSet, FILE* csv_in, char * columnToSort) {
    int i,j;
    int columnToSortIndex, validNumRows;
    rowCountforFile = 0;
    int rowIndex = 0;
    char* columnToSortType=NULL;
    char* line = NULL;
    char* token = NULL, *tokenType=NULL;            
    size_t size = 128;    

    //Get index of the column we want to sort by
    columnToSortIndex = isValidColumn(columnToSort);

    if(columnToSortIndex == -1) {
        printf("ERROR COLUMN IS NOT VALID!\n");
        exit(1);
    }
    columnToSortType = validColumnTypes[columnToSortIndex];
    
    if(NULL == csv_in){
        printf("NULL FILE");
    }
    //Skip first row
    line = (char *)malloc(1024 * sizeof(char));
    getline(&line, &size, csv_in);

    int c;
    while(!feof(csv_in)) {
        int colIndex = 0;
        int i;

        line = NULL;
        getline(&line, &size, csv_in);  

        //Begining of a new row starts here
        Row *tempRow = malloc(sizeof(struct Row)*28);
        if(tempRow != NULL) {
            rowSet[rowIndex] = tempRow;
        } else {

        }
        
        //Gets first column
        token = strtok_single(line, ",\n");
        if(!token)
            break;
        tokenType = findType(token, colIdx);

        //Set the values in the rows matrix
        if(token==NULL || strcmp(token,"")==0){
                    token="<NULL>";
        }  
        if(tokenType==NULL || strcmp(tokenType,"")==0){
                    tokenType="<NULL>";
        }  
        char *tmp = malloc(strlen(token) + 1);
        if (tmp != NULL) {
            rowSet[rowIndex]->colEntries[colIndex].value = tmp;
            strcpy(rowSet[rowIndex]->colEntries[colIndex].value, token);
            //printf("row value '%s' \n", rowSet[rowIndex]->colEntries[colIndex].value);
        } else {
            printf("FAILURE on first tmp\n");
        }

        char *tmpType = malloc(strlen(tokenType) + 1);
        if( tmpType != NULL) {
            rowSet[rowIndex]->colEntries[colIndex].type = tmpType;
            strcpy(rowSet[rowIndex]->colEntries[colIndex].type, tokenType);
            //if(rowSet[rowIndex]->colEntries[colIndex].type==NULL || strcmp(rowSet[rowIndex]->colEntries[colIndex].type,"")==0){
                    //rowSet[rowIndex]->colEntries[colIndex].type="<NULL>";
                //}
            //printf("row type '%s' \n", rowSet[rowIndex]->colEntries[colIndex].type);
        } else {
            printf("FAILURE on first tmptype\n");
        }
        colIndex++;
                
        while(token) {
            //Tokenizes the string based on ','
            //Starts from first column onward until end
            //If the first character in the line is a " then we tokenize based on the quotation mark
            token = strtok_single(NULL, DELIMITERS);
            if(!token)
                break;
            
            tokenType = findType(token, colIdx);
            if(token==NULL || strcmp(token,"")==0){
                    token="<NULL>";
            }  
            if(tokenType==NULL || strcmp(tokenType,"")==0){
                        tokenType="<NULL>";
            } 
            //Set the values in the rowSet matrix

            char *tmp2 = malloc(strlen(token) + 1);
            if (tmp2 != NULL) {
                rowSet[rowIndex]->colEntries[colIndex].value = tmp2;
                strcpy(rowSet[rowIndex]->colEntries[colIndex].value, token);
               // if(rowSet[rowIndex]->colEntries[colIndex].value==NULL || strcmp(rowSet[rowIndex]->colEntries[colIndex].value,"")==0){
                    //rowSet[rowIndex]->colEntries[colIndex].value="<NULL>";
                //}
                //printf("row value '%s' \n", rowSet[rowIndex]->colEntries[colIndex].value);
            } else {
                printf("FAILURE on temp\n");
            }

            char *tmpType2 = malloc(strlen(tokenType) + 1);
            if(tmpType2 != NULL) {
                rowSet[rowIndex]->colEntries[colIndex].type = tmpType2;
                strcpy(rowSet[rowIndex]->colEntries[colIndex].type, tokenType);
                //if(rowSet[rowIndex]->colEntries[colIndex].type==NULL || strcmp(rowSet[rowIndex]->colEntries[colIndex].type,"")==0){
                    //rowSet[rowIndex]->colEntries[colIndex].type="<NULL>";
                //}
                //printf("row type '%s' \n", rowSet[rowIndex]->colEntries[colIndex].type);
            } else {
                printf("FAILURE on tempType\n");
            }
            colIndex++;
        }
        rowIndex++;
    }

    validNumRows = rowIndex;
    setNumberofRows(validNumRows);

    //Implement the sorting and call here
    doSort(rowSet,columnToSortIndex,columnToSortType,validNumRows);

    //Print to a CSV file
    //fprintf(csv_out, headerLine);
    //printToCSV(csv_out, rowSet, validNumRows, NUM_COLS);

    //fclose(csv_out);
    free(line);
    fclose(csv_in);
        
    return;
    //end of function
}

void printToCSV(FILE *csv_out, Row ** rows, int validNumRows, int validNumCols) {
    //Print the header line.
    fprintf(csv_out, HEADER_LINE);

    int i=0,j=0;
    //Loop through the rows
    for (i = 0; i < validNumRows; i++) {
        //Loop through the columns
        for(j = 0; j < validNumCols-1; j++) {
            if(rows[i]->colEntries != NULL) {
                fprintf(csv_out, "%s,", rows[i]->colEntries[j].value);
            } else {
                fprintf(csv_out, "<NULL>,");
            }
        }

        if(rows[i]->colEntries != NULL) {
            fprintf(csv_out, "%s\n", rows[i]->colEntries[j].value);
        } else {
            fprintf(csv_out, "<NULL>\n");
        }
    } 

    fclose(csv_out);
}

void setNumberofRows(int validNumRows){
    rowCountforFile = validNumRows;
}

int getNumberofRows(){
    return rowCountforFile;
}

//Returns index of valid column, if not valid return -1
int isValidColumn(char* columnName) {
    int i;
    for(i = 0; i < (sizeof(validColumns) / sizeof(validColumns[i])); i++){
        if(strcmp(validColumns[i], columnName) == 0) {
            return i;
        }
    }
    return -1;
}

//Returns the type of a given string token, does NOT perfom atoi() or atof()
char * findType(char* token, int colIdx) {
    int length,i;

    if(strcmp(token,"") == 0) {
        return validColumnTypes[colIdx];
    }

    length = strlen (token);
    for (i = 0; i < length; i++) {
        if(token[i] != '\r') {
            //If a character is not a digit but it is also not a period it must be a string
            if (!isdigit(token[i]) && token[i] != '.') {
                return "char";
            } else if (!isdigit(token[i])){ //otherwise it is a float
                return "float";
            } 
        }
            
    } //If we never encounter a non-numerical it must be an integer
    return "int";
}


//Tokenize a given input based on a delimiter, returns NULL if consecutive delimiters
char* strtok_single (char * string, char const * delimiter) {
    static char *source = NULL;
    char *p = NULL, *result = 0;
    if(string != NULL)         source = string;
    if(source == NULL)         return NULL;
    
    p = strpbrk(source, delimiter);

    if(p != NULL) {
        char *s = p+1;
        if(*(s) == '"') { //Enountered a movie title that has commas in it
            while(*(s+1) != '"') { //Go through util the next " and replace the commas with ;
                if(*(s) == ',') {
                    *s = ';';
                }
                s++;
            }
        }

       *p  = 0;
       result = source;
       source = ++p;
    }
 return result;
}
