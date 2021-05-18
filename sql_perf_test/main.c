/*
 * This code is based on https://blog.csdn.net/fuqiangnxn/article/details/94158465
 */
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#if defined _WIN64 || defined _WIN32
#include <windows.h>
#include <conio.h>
#include <tchar.h>
#include <sal.h>
#include "include/getopt.h"
#else
#include <unistd.h>
#include <sys/time.h>
#include <string.h>
 //#include <gperftools/profiler.h>
#endif
#include <sql.h>
#include <sqlext.h>

/* 参数说明
-t 客户端连接数量
-d dml类型 i u s d
-p 0-无prepare，其他-有prepare
-c 多少条Sql进行commit
-n 每个客户端执行的SQL数量
*/

#define ERRMSG_LEN 1024
#define bool unsigned short

#ifdef __GNUC__
#define atomic_inc(ptr, val) __sync_fetch_and_add ((ptr), (val))
#elif defined (_WIN32) || defined (_WIN64)
#define atomic_inc(ptr, val) InterlockedAdd ((ptr), (val))
#else
#error "Need some more porting work here"
#endif

typedef enum {
    false = 0,
    true
} booltype;

//执行SQL的类型
typedef enum {
    dml_unknown = 0,
    dml_insert,
    dml_update,
    dml_select,
    dml_delete
} dmltype;

static int g_query_succ_num = 0;
static int g_query_fail_num = 0;
static int g_connect_num = 0;   //客户端连接数量
static int g_connect_failed = 0;    //客户端连接失败数量
static int g_connect_success = 0;   //客户端连接成功数量
static long g_commit_num = 0;   //多少条sql进行commit
static long g_round_num = 0;    //每个客户端执行的SQL数量
static long g_sql_missing = 0;  //执行sql未命中的总数量
static long g_throughput = 0;   //吞吐量计算方式的一种
static dmltype g_dmltype = dml_unknown;
static bool g_isprepare = false;    //使用prepare方式与否
SQLCHAR *g_dsn = NULL;

/*
 * gettimeofday() for windows: https://gist.github.com/ugovaretto/5875385
 */
#if defined (_WIN64) || defined (_WIN32)

#if defined(_MSC_VER) || defined(_MSC_EXTENSIONS)
#define DELTA_EPOCH_IN_MICROSECS  11644473600000000Ui64
#else
#define DELTA_EPOCH_IN_MICROSECS  11644473600000000ULL
#endif

struct timezone
{
    int  tz_minuteswest; /* minutes W of Greenwich */
    int  tz_dsttime;     /* type of dst correction */
};

static int gettimeofday(struct timeval* tv, struct timezone* tz)
{
    FILETIME ft;
    unsigned __int64 tmpres = 0;
    static int tzflag = 0;

    if (NULL != tv)
    {
        GetSystemTimeAsFileTime(&ft);

        tmpres |= ft.dwHighDateTime;
        tmpres <<= 32;
        tmpres |= ft.dwLowDateTime;

        tmpres /= 10;  /*convert into microseconds*/
        /*converting file time to unix epoch*/
        tmpres -= DELTA_EPOCH_IN_MICROSECS;
        tv->tv_sec = (long)(tmpres / 1000000UL);
        tv->tv_usec = (long)(tmpres % 1000000UL);
    }

    if (NULL != tz)
    {
        if (!tzflag)
        {
            _tzset();
            tzflag++;
        }
        tz->tz_minuteswest = _timezone / 60;
        tz->tz_dsttime = _daylight;
    }

    return 0;
}
#endif

/* Get Current Time to microsecond */
static double GetUTimeToDouble()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + 0.000001 * tv.tv_usec;
}

static bool CheckError(SQLRETURN mRet, SQLSMALLINT mHandleType, SQLHANDLE mHandle, SQLCHAR* mErrMsg)
{
    SQLRETURN       retcode = SQL_SUCCESS;

    SQLSMALLINT     errNum = 1;
    SQLCHAR         *sqlState = (SQLCHAR*)malloc(6 * sizeof(SQLCHAR));
    SQLINTEGER      nativeError;
    SQLCHAR         *errMsg = (SQLCHAR*)malloc(ERRMSG_LEN * sizeof(SQLCHAR));
    SQLSMALLINT     textLengthPtr;

    if (!sqlState || !errMsg) {
        return false;
    }

    if ((mRet != SQL_SUCCESS) && (mRet != SQL_SUCCESS_WITH_INFO)) {
        while (retcode != SQL_NO_DATA) {
            retcode = SQLGetDiagRec(mHandleType, mHandle, errNum, sqlState, &nativeError, errMsg, ERRMSG_LEN, &textLengthPtr);

            if (retcode == SQL_INVALID_HANDLE) {
                fprintf(stderr, "CheckError function was called with an invalid handle!!\n");
                return true;
            }

            if ((retcode == SQL_SUCCESS) || (retcode == SQL_SUCCESS_WITH_INFO)) {
                fprintf(stderr, "ERROR: %d:  %s : %s \n", nativeError, sqlState, errMsg);
            }

            errNum++;
        }

        fprintf(stderr, "%s\n", mErrMsg);
        return true;    /* all errors on this handle have been reported */
    } else {
        return false;   /* no errors to report */
    }
}

static SQLRETURN ConnectDB(SQLHDBC* mHdbc)
{
    SQLRETURN       rc;
    SQLCHAR         verInfoBuffer[255];
    SQLSMALLINT     verInfoLen;
    char            errMsg[ERRMSG_LEN];
    unsigned int    tid = (unsigned int)pthread_self().p;

    /* Establish the database connection */
    printf("dsn = %s\n", g_dsn);
    rc = SQLConnect(*mHdbc, g_dsn, SQL_NTS, "gpadmin", SQL_NTS, "gpadmin", SQL_NTS);
    snprintf(errMsg, ERRMSG_LEN, "[%u] SQLConnect failed!\n", tid);
    if (CheckError(rc, SQL_HANDLE_DBC, *mHdbc, (SQLCHAR*)errMsg)) {
        return rc;
    }

    /* Get version information from the database server */
    rc = SQLGetInfo(*mHdbc, SQL_DBMS_VER, verInfoBuffer, 255, &verInfoLen);
    snprintf(errMsg, ERRMSG_LEN, "[%u] SQLGetInfo<SQL_dbms_ver> failed!\n", tid);
    if (CheckError(rc, SQL_HANDLE_DBC, *mHdbc, (SQLCHAR*)errMsg)) {
        return rc;
    }

    /* Set the connection attribute SQL_ATTR_AUTOCOMMIT to SQL_AUTOCOMMIT_OFF */
    rc = SQLSetConnectAttr(*mHdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, (SQLINTEGER)0);
    snprintf(errMsg, ERRMSG_LEN, "[%u] SQLSetConnectAttr<SQL_ATTR_AUTOCOMMIT> failed!\n", tid);
    if (CheckError(rc, SQL_HANDLE_DBC, *mHdbc, (SQLCHAR*)errMsg)) {
    return rc;
    }

    return SQL_SUCCESS;
}

static void* Workload(void* mPartitionID)
{
    //ProfilerStart("out.prof");
    SQLHENV henv = SQL_NULL_HANDLE;
    SQLHDBC hdbc = SQL_NULL_HANDLE;
    SQLHSTMT hstmt = SQL_NULL_HANDLE;

    SQLRETURN rc;
    char strSql[1000];
    long int userid = -1;
    int partition_id;
    char errMsg[ERRMSG_LEN];

    double time_start, time_end;
    unsigned int tid;
    int i;

    tid = (unsigned int)pthread_self().p;
    partition_id = *(int*)mPartitionID;

    /* Allocate the Environment handle */
    rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (rc != SQL_SUCCESS) {
        fprintf(stdout, "[%u] Environment Handle Allocation failed!!\n", tid);
        goto exit;
    }

    /* Set the ODBC version to 3.0 */
    rc = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    snprintf(errMsg, ERRMSG_LEN, "[%u] SQLSetEnvAttr<ODBC_VERSION> failed!!\n", tid);
    if (CheckError(rc, SQL_HANDLE_ENV, henv, (SQLCHAR*)errMsg)) {
        goto exit;
    }

    /* Allocate the connection handle */
    rc = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    snprintf(errMsg, ERRMSG_LEN, "[%u] Connection Handle Allocation failed!!\n", tid);
    if (CheckError(rc, SQL_HANDLE_ENV, henv, (SQLCHAR*)errMsg)) {
        goto exit;
    }

    /* Workload Start */
    rc = ConnectDB(&hdbc);
    if ((rc != SQL_SUCCESS) && (rc != SQL_SUCCESS_WITH_INFO)) {
        atomic_inc(&g_connect_failed, 1);
        goto exit;
    }
    else {
        atomic_inc(&g_connect_success, 1);
    }

    /* Allocate the statement handle */
    rc = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    snprintf(errMsg, ERRMSG_LEN, "[%u] Statement Handle Allocation failed!\n", tid);
    if (CheckError(rc, SQL_HANDLE_DBC, hdbc, (SQLCHAR*)errMsg)) {
        goto exit;
    }

    if (g_isprepare) {
        /* Prepare Statement */
        switch (g_dmltype) {
        case dml_insert:
            snprintf(strSql, 1000, "insert into t1 values (?,'aaaaaaaaaaaaaaaaaaaaaaaaaaa','2021-04-09',1)");
            break;
        case dml_update:
            snprintf(strSql, 1000, "update t1 set cause = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbb',modifytime = '2021-04-10',version = 2 where id = ?");
            break;
        case dml_select:
            snprintf(strSql, 1000, "select * from t1 where id = ?");
            break;
        case dml_delete:
            snprintf(strSql, 1000, "delete from t1 where id = ?");
            break;
        default:
            break;
        }

        rc = SQLPrepare(hstmt, (SQLCHAR*)strSql, sizeof(strSql));
        snprintf(errMsg, ERRMSG_LEN, "[%u] SQLPrepare '%s' failed!\n", tid, strSql);
        if (CheckError(rc, SQL_HANDLE_STMT, hstmt, (SQLCHAR*)errMsg))
            goto exit;

        rc = SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 8, 0, (SQLBIGINT*)&userid, 0, NULL);
        snprintf(errMsg, ERRMSG_LEN, "[%u] SQLBindParameter (param 1) failed!\n", tid);
        if (CheckError(rc, SQL_HANDLE_STMT, hstmt, (SQLCHAR*)errMsg)) {
            goto exit;
        }
    }

    time_start = GetUTimeToDouble();
    for (i = 0; i < g_round_num; i++) {
        //userid = i + partition_id * g_round_num;
        userid = g_round_num % 7 + 1;

        if (g_isprepare) {
            rc = SQLExecute(hstmt);
            if (!(rc == SQL_SUCCESS) || (rc == SQL_SUCCESS_WITH_INFO)) {
                snprintf(errMsg, ERRMSG_LEN, "[%u] SQLExecute failed!\n", tid);
                if (CheckError(rc, SQL_HANDLE_STMT, hstmt, (SQLCHAR*)errMsg)) {
                    goto exit;
                }
            }
        } else {
            switch (g_dmltype) {
            case dml_insert:
                snprintf(strSql, 1000, "insert into t1 values (%ld,'aaaaaaaaaaaaaaaaaaaaaaaaaaa','2018-10-11',1)", userid);
                break;
            case dml_update:
                snprintf(strSql, 1000, "update t1 set cause = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbb',modifytime = '2018-11-12',version = 2 where id = %ld", userid);
                break;
            case dml_select:
                snprintf(strSql, 1000, "select * from lineitem where l_linenumber=%ld", userid);
                break;
            case dml_delete:
                snprintf(strSql, 1000, "delete from t1 where id=%ld", userid);
                break;
            default:
                break;
            }

            rc = SQLExecDirect(hstmt, (SQLCHAR*)strSql, SQL_NTS);
            if (!(rc == SQL_SUCCESS) || (rc == SQL_SUCCESS_WITH_INFO)) {
                snprintf(errMsg, ERRMSG_LEN, "[%u] SQLExecDriect failed!\n", tid);
                if (CheckError(rc, SQL_HANDLE_STMT, hstmt, (SQLCHAR*)errMsg)) {
                    goto exit;
                }
            }
        }

        if (g_dmltype == dml_select) {
            rc = SQLFetch(hstmt);
            if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
                //SQLBIGINT Value1 = 0;
                //SQLCHAR Value2[50];
                //memset(Value2, 0, sizeof(Value2));
                //SQLLEN  len1 = 1, len2 = 1;
                //rc = SQLGetData(hstmt, 1, SQL_C_SBIGINT, &Value1, 0, (SQLLEN*)&len1);
                //rc = SQLGetData(hstmt, 2, SQL_C_CHAR, Value2, sizeof(Value2), (SQLLEN*)&len2);
                //printf("[%u] id:%ld results:value1:%ld,values2:%s,len1:%d,len2:%d\n",
                //    tid, userid, (long)Value1, Value2, (int)len1, (int)len2);
                g_query_succ_num++;
            } else {
                g_query_fail_num++;
                if (rc == SQL_NO_DATA) {
                    atomic_inc(&g_sql_missing, 1);
                } else {
                    snprintf(errMsg, ERRMSG_LEN, "[%u] SELECT SQLFetch failed!\n", tid);
                    if (CheckError(rc, SQL_HANDLE_STMT, hstmt, (SQLCHAR*)errMsg)) {
                        goto exit;
                    }                    
                }
            }

            rc = SQLCloseCursor(hstmt);
            if (!(rc == SQL_SUCCESS) || (rc == SQL_SUCCESS_WITH_INFO)) {
                snprintf(errMsg, ERRMSG_LEN, "[%u] SQLCloseCursor failed!\n", tid);
                if (CheckError(rc, SQL_HANDLE_STMT, hstmt, (SQLCHAR*)errMsg))
                    goto exit;
            }
        } else {
            if (i % g_commit_num == 0) {
                rc = SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_COMMIT);
                snprintf(errMsg, ERRMSG_LEN, "[%u] DCL SQLEndTran failed!\n", tid);
                if (CheckError(rc, SQL_HANDLE_STMT, hstmt, (SQLCHAR*)errMsg)) {
                    goto exit;
                }
            }
        }
    }

    if (g_dmltype != dml_select) {
        if (g_round_num % g_commit_num != 0) {
            rc = SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_COMMIT);
            snprintf(errMsg, ERRMSG_LEN, "[%u] DCL SQLEndTran failed!\n", tid);
            if (CheckError(rc, SQL_HANDLE_STMT, hstmt, (SQLCHAR*)errMsg)) {
                goto exit;
            }
        }
    }

    time_end = GetUTimeToDouble();
    //printf("[%u] ---time:%lf,throughput:%lf---\n",
   //     tid, time_end - time_start, (double)g_round_num / (double)(time_end - time_start));
    atomic_inc(&g_throughput, (long)((double)g_round_num / (double)(time_end - time_start)));
    /* Workload End */
    //ProfilerStop();
exit:
    /* Close the statement handle */
    SQLFreeStmt(hstmt, SQL_CLOSE);

    /* Free the statement handle */
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

    /* Disconnect from the data source */
    SQLDisconnect(hdbc);

    /* Free the environment handle and the database connection handle */
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);
    return NULL;
}

void display_usage(void)
{
    printf("select -t <client_num> -d <dml_type> -n <sql num per client> \n");
    printf("select -c <how many sql to commit> -p <prepare or not>  \n");
    exit(EXIT_FAILURE);
}

int main(int argc, char* argv[])
{
    double  time_end, time_start = GetUTimeToDouble();
    int     opt = -1;
    int i;

    while ((opt = getopt(argc, argv, "t:D:d:n:c:p:h:?:")) != -1) {
        switch (opt) {
        case 't':
            g_connect_num = atoi(optarg);
            break;
        case 'D':
            g_dsn = atoi(optarg) == 1 ? "gpdbdemo-dd" : "gpdbdemo-psql";
            break;
        case 'd':
            switch (*optarg) {
            case 'i':
                g_dmltype = dml_insert;
                break;
            case 'u':
                g_dmltype = dml_update;
                break;
            case 's':
                g_dmltype = dml_select;
                break;
            case 'd':
                g_dmltype = dml_delete;
                break;
            default:
                printf("Unknown options!\n");
                display_usage();
            }
            break;
        case 'n':
            g_round_num = atoi(optarg);
            break;
        case 'c':
            g_commit_num = atoi(optarg);
            break;
        case 'p':
            if (atoi(optarg) != 0) {
                g_isprepare = true;
            }
            break;
        case 'h':
        case '?':
            display_usage();
            break;
        default:
            printf("%d\n", opt);
            printf("unknown options is founded!\n");
            display_usage();
            break;
        }
    }

    if (optind < argc) {
        printf("non-option ARGV-elements: ");
        while (optind < argc) {
            printf("%s ", argv[optind++]);
        }
        printf("\n");
    }

    /* Check Arguments */
    if (g_connect_num < 0) {
        printf("client number error!\n");
        exit(EXIT_FAILURE);
    }

    if (g_round_num < 1) {
        printf("round numer error!\n");
        exit(EXIT_FAILURE);
    }

    if (g_commit_num < 1) {
        printf("commit num err!\n");
        exit(EXIT_FAILURE);
    }

    if (!g_dsn) {
        printf("use default dsn gpdbdemo_psql\n");
        g_dsn = "gpdbdemo-psql";
    }

    pthread_t *tid = (pthread_t*)malloc(g_connect_num * sizeof(pthread_t));
    int *partition_id = (int*)malloc(g_connect_num * sizeof(int));
    if (!tid || !partition_id) {
        return 0;
    }

    /* Generate Workload Thread */
    printf("Start with Client:%d,DmlType:%d,Round:%ld,Commit:%ld,Prepare:%d\n",
        g_connect_num, g_dmltype, g_round_num, g_commit_num, g_isprepare);

    for (i = 0; i < g_connect_num; i++) {
        partition_id[i] = i;
        if (0 != pthread_create(&tid[i], NULL/*&attr*/, Workload, (void*)&partition_id[i])) {
            printf("pthread_create %d failed ---\n", i);
            exit(EXIT_FAILURE);
        }
    }

    for (i = 0; i < g_connect_num; i++) {
        pthread_join(tid[i], NULL);
    }

    time_end = GetUTimeToDouble();
    printf("connect--success:%d,failed:%d..sql--missing:%ld,throughput:%ld\n",
        g_connect_success, g_connect_failed, g_sql_missing, g_throughput);
    printf("end---total---time:%lf,throughput:%lf\n",
        time_end - time_start, (double)g_round_num / (double)(time_end - time_start));
    return 0;
}