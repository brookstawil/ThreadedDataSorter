#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#define NUM_COLS 28
#define NUM_ROWS 8192

void mergeSort(Row ** rows, int l, int r);
void merge(Row ** rows, int l, int m, int r);
void mergeRowsTwoFinger(Row **sortedRows, Row ** row1, Row ** row2);
int getAmountOfRows(Row **rowSet1);
char *colType;
int colIdx;
long doCompare(Row *row1, Row *row2);
int isValidType(Row *row);

int doSort(Row ** rows, int sortColIdx, char * sortColType, int arrSize){
    colIdx = sortColIdx;
    colType = sortColType;
    
    mergeSort(rows, 0, arrSize-1);
    
    return 0;
}

void mergeSort(Row **rows, int l, int r) {
    if (l < r){
        
        int m=(l+(r-1))/2;
        
        mergeSort(rows, l, m);
        
        mergeSort(rows, m+1, r);
        
        merge(rows, l, m, r);
    }
    
}

int getAmountOfRows(Row **rowSet1) {
    int k = 0;
    while(isValidType(rowSet1[k]) != 0) {
        k++;
    }
    return k;
}

void mergeRowsTwoFinger(Row **sortedRows, Row **row1, Row **row2) {
    /*
    algorithm merge(A, B) is
        inputs A, B : list
        returns list

        C := new empty list
        while A is not empty and B is not empty do
            if head(A) ≤ head(B) then
                append head(A) to C
                drop the head of A
            else
                append head(B) to C
                drop the head of B

        // By now, either A or B is empty. It remains to empty the other input list.
        while A is not empty do
            append head(A) to C
            drop the head of A
        while B is not empty do
            append head(B) to C
            drop the head of B

        return C
    */
    int i = 0,j = 0,k = 0;
    int a, b;

    while(isValidType(row1[i]) != 0 && isValidType(row2[j]) != 0) {
        if (doCompare(row1[i], row2[j]) <= 0) {
            sortedRows[k] = row1[i];
            i++;
        } else {
            sortedRows[k] = row2[j];
            j++;
        }
        k++;
    }

    while(isValidType(row1[i]) != 0) {
        sortedRows[k] = row1[i];
        i++;
        k++;
    }

    while(isValidType(row2[j]) != 0) {
        sortedRows[k] = row2[j];
        j++;
        k++;
    }

    return;
}

int isValidType(Row *row) {
    if(row == NULL) {
        return 0;
    } else {
        return 1;
    }

}

void merge(Row **row, int left, int mid, int right) {
	int i,j,k;
    int n1 = mid - left + 1;
    int n2 =  right - mid;
    
    Row *L[n1], *R[n2];
    
    for (i = 0; i < n1; i++) {
		L[i] = row[left + i];	
	}
    for (j = 0; j < n2; j++) {
		R[j] = row[mid + 1 + j];
	}
        
	i = 0;
	j = 0;
	k = left;
    while (i < n1 && j < n2) {
        if (doCompare(L[i], R[j]) <= 0) {
            row[k] = L[i];
            i++;
        } else {
            row[k] = R[j];
            j++;
        }
        k++;
    }
    
	while (i < n1) {
        row[k] = L[i];
        i++;
        k++;
    }
    
    while (j < n2) {
        row[k] = R[j];
        j++;
        k++;
    }
    
}


long doCompare(Row *row1, Row *row2) {
    const char* r1Value = (row1->colEntries)[colIdx].value;
    const char* r2Value = (row2->colEntries)[colIdx].value;
    if(r1Value == NULL && r2Value == NULL){
        return 0;
    }
    if(r1Value == NULL || strcmp(r1Value,"")==0){
        r1Value == "<NULL>";
        //return strcmp("<NULL>",r2Value);
        return 1;
    }
    if(r2Value == NULL || strcmp(r2Value,"")==0){
        r2Value == "<NULL>";
        //return strcmp("<NULL>",r1Value);
        return 1;
    }

    if (strcmp(colType, "char") == 0) {
        if (*r1Value == '"') {
            r1Value++;
        }
        if (*r2Value == '"') {
            r2Value++;
        }
        return(strcmp(r1Value, r2Value));
    } else if (strcmp(colType, "int") == 0){
                int i1Value = atoi(r1Value);
                int i2Value = atoi(r2Value);
                return (i1Value - i2Value);
    } else if (strcmp(colType, "long") == 0) {
                long l1Value = atol(r1Value);
                long l2Value = atol(r2Value);
                return (l1Value - l2Value);
    } else if (strcmp(colType, "float") == 0) {
                float f1Value = atof(r1Value);
                float f2Value = atof(r2Value);
                return (f1Value - f2Value);
    } else {
        fprintf(stderr, "Unknown colType: %s\n", colType);
        exit(1);
    }
}
