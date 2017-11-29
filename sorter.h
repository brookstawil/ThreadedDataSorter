typedef struct rowEntry {
    const char * value;
    const char * type;
} rowEntry;

typedef struct Row {
    struct rowEntry* colEntries;
} Row;

size_t parseline(char** lineptr, size_t *n, FILE *stream);
char* strtok_single(char* str, char const* delims);
char* findType(char* token, int colIdx);
int isValidColumn(char* columnName);
void printToCSV(FILE *csv_out, Row ** rows, int numRows, int numCols);
void sortnew(Row ** rowSet, FILE *fp_in, char * columnToSort);
void setNumberofRows(int validNumRows);
int getNumberofRows();
