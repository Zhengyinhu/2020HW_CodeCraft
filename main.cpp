#include <iostream>
#include <fstream>
#include <stdio.h>
#include <string.h>

#include <vector>
#include <unordered_map>
#include <stack>

#include <algorithm>
#include <chrono>

#include <thread>

#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>

#include <sys/syscall.h>
#include <sched.h>

#define CORE_NUM 8	//线程数
#define MAX_NODE 2000000 // 最大节点数
#define MAX_DATA_ROW 2000000	//读入文件最大行数

#define MAX_LOOP3_NUM 1000000	//3环的最大个数
#define MAX_LOOP4_NUM 1000000	//4环的最大个数
#define MAX_LOOP5_NUM 3000000	//5环的最大个数
#define MAX_LOOP6_NUM 4000000	//6环的最大个数
#define MAX_LOOP7_NUM 5000000	//7环的最大个数

#define DISTANCE_1 1
#define DISTANCE_2 2
#define DISTANCE_3 4
#define VISITED    8
#define UN_VISITED 247

#define FIVE_LEVEL 7
#define SIX_LEVEL 3

/*两者只能开启一个*/
//#define TEST 1	//测试模式
#define RUN 1	//全速模式

/*是否开启CPU绑核*/
#define CPU_AFFINITY 1
using namespace std;
using namespace chrono;

/*存放多线程输出结果*/
struct RESULT {
	int result7[7*MAX_LOOP7_NUM];
	int result6[6*MAX_LOOP6_NUM];
	int result5[5*MAX_LOOP5_NUM];
	int result4[4*MAX_LOOP4_NUM];
	int result3[3*MAX_LOOP3_NUM];
	int circleCnt[5][MAX_NODE+1];
	int* result[5] = { result3,result4,result5,result6,result7 };
	int rowCnt[5] = { 0 };
	long long size;
}Result[CORE_NUM];


/*-------------------------------------------------------------------------------------------------------------------------
 *功能：用于缓存读入文件的字符串
 *str：字符串首地址
 *len：字符串的长度
---------------------------------------------------------------------------------------------------------------------------*/
struct strBuf {
	char* str;
	int len;
}strBufs2[MAX_DATA_ROW], strBufs[MAX_DATA_ROW+1];

int validNodeNumber;	//节点ID去重后的有效节点数
int validDataRow;	//有效数据行数

/*-------------------------------------------------------------------------------------------------------------------------
 *功能：存储入度地图和出度地图，validNodeNumber*MAX_EDGE二维数组，以映射后的节点ID1为行数索引，每行中存储ID1为起点的边
 *validMapOutIndex/validMapInIndex：每次存储新的边，对应下标的validMapOutIndex/validMapInIndex将会+1
---------------------------------------------------------------------------------------------------------------------------*/
int (*outEdgeIndex)[2] = nullptr;
int(*inEdgeIndex)[2] = nullptr;

/*-------------------------------------------------------------------------------------------------------------------------
 *功能：存储原始的startid和endid，按照读入顺序依次存入allNode,_allNode，每存储两个id后，将存储对应的转账金额入allMoney数组
 *allNodeCnt：读入节点的个数，eg：200行数据，将会有400个节点
---------------------------------------------------------------------------------------------------------------------------*/
int forwardData[MAX_DATA_ROW][3];
int reverseData[MAX_DATA_ROW][3];

unordered_map<int, int> tranIndex;//原始到id到有序id的映射，eg：2,8,12,14->0,1,2,3
unordered_map<int, int> stringIndex; // id到字符串地址数组下标的映射


#define save3Cirecle(id0,id1,id2,threadid) \
	int& curPos = Result[threadId].rowCnt[0]; \
	RESULT& _result = Result[threadid];	\
	_result.circleCnt[0][id0]++;	/*first为首节点，在对应环长度的个数统计*/ \
	_result.result[0][curPos++] = id0; /*存储0节点的字符串地址*/\
	_result.result[0][curPos++] = id1; /*存储1节点的字符串地址*/\
	_result.result[0][curPos++] = id2; /*存储2节点的字符串地址*/\
	_result.size += strBufs[id0].len + strBufs[id1].len + strBufs[id2].len + 3; /*统计该环的长度*/

#define save4Cirecle(id0,id1,id2,id3,threadid) \
	int& curPos = Result[threadId].rowCnt[1]; \
	RESULT& _result = Result[threadid];	\
	_result.circleCnt[1][id0]++;	 \
	_result.result[1][curPos++] = id0;  \
	_result.result[1][curPos++] = id1; \
	_result.result[1][curPos++] = id2; \
	_result.result[1][curPos++] = id3; \
	_result.size += strBufs[id0].len + strBufs[id1].len + strBufs[id2].len + strBufs[id3].len + 4;

#define save5Cirecle(id0,id1,id2,id3,id4,threadid) \
	int& curPos = Result[threadId].rowCnt[2]; \
	RESULT& _result = Result[threadid];	\
	_result.circleCnt[2][id0]++;	 \
	_result.result[2][curPos++] = id0;  \
	_result.result[2][curPos++] = id1; \
	_result.result[2][curPos++] = id2; \
	_result.result[2][curPos++] = id3; \
	_result.result[2][curPos++] = id4; \
	_result.size += strBufs[id0].len + strBufs[id1].len + strBufs[id2].len + strBufs[id3].len + strBufs[id4].len+ 5;

#define save6Cirecle(id0,id1,id2,id3,id4,id5,threadid) \
	int& curPos = Result[threadId].rowCnt[3]; \
	RESULT& _result = Result[threadid];	\
	_result.circleCnt[3][id0]++;	 \
	_result.result[3][curPos++] = id0;  \
	_result.result[3][curPos++] = id1; \
	_result.result[3][curPos++] = id2; \
	_result.result[3][curPos++] = id3; \
	_result.result[3][curPos++] = id4; \
	_result.result[3][curPos++] = id5; \
	_result.size += strBufs[id0].len + strBufs[id1].len + strBufs[id2].len + strBufs[id3].len + strBufs[id4].len + strBufs[id5].len+ 6;

#define save7Cirecle(id0,id1,id2,id3,id4,id5,id6,threadid) \
	int& curPos = Result[threadId].rowCnt[4]; \
	RESULT& _result = Result[threadid];	\
	_result.circleCnt[4][id0]++;	 \
	_result.result[4][curPos++] = id0;  \
	_result.result[4][curPos++] = id1; \
	_result.result[4][curPos++] = id2; \
	_result.result[4][curPos++] = id3; \
	_result.result[4][curPos++] = id4; \
	_result.result[4][curPos++] = id5; \
	_result.result[4][curPos++] = id6; \
	_result.size += strBufs[id0].len + strBufs[id1].len + strBufs[id2].len + strBufs[id3].len + \
					strBufs[id4].len + strBufs[id5].len+ strBufs[id6].len+ 7;

#define findMinIndex(cur,first,startIndex) \
	for (startIndex = outEdgeIndex[cur][0]; startIndex < outEdgeIndex[cur][1]; ++startIndex) { \
		if (forwardData[startIndex][1] > first) break;	\
	}
	

/*-------------------------------------------------------------------------------------------------------------------------
 *功能：整型转字符串,并将结果存入dest中
 *dest：目标char数组地址
 *num：待转换的int数据
---------------------------------------------------------------------------------------------------------------------------*/
int myitoa(char* dest, int num);
/*-------------------------------------------------------------------------------------------------------------------------
 *功能：字符串转整型int
 *nptr：字符串首地址
 *返回值：整型int
---------------------------------------------------------------------------------------------------------------------------*/
int myatoi(const char *nptr);
void CreateGraph();	//创建图
int compare_1(const void *t1, const void *t2);/*首次出度地图排序，按照第一列升序排序*/
int compare_2(const void *t1, const void *t2);/*出度地图排序,第一列进行升序排序,如果第一列相等，则按照第二列进行升序排序*/
int compare_3(const void *t1, const void *t2);/*入度地图排序,第一列进行升序排序,如果第一列相等，则按照第二列进行降序排序*/
void myunique();//二维数组去重
void findCircle(int threadId); //找环 threadId：线程号
void SaveResultData(string& resultFile);	//保存结果数据 resultFile：结果数据路径
void LoadTestData(string& testFile);//加载测试数据  testFile：测试数据路径

void RecordDFSIn(int first, char* record, int* lastMoney);
void PDFS(int first, char* record, int* lastMoney, int threadId);

/*首次出度地图排序，按照第一列升序排序*/
int compare_1(const void *t1, const void *t2) {
	return ((int*)t1)[0] - ((int*)t2)[0];
}
/*出度地图排序,第一列进行升序排序,如果第一列相等，则按照第二列进行升序排序*/
int compare_2(const void *t1, const void *t2) {
	return ((int*)t1)[0] == ((int*)t2)[0] ? ((int*)t1)[1] - ((int*)t2)[1] : ((int*)t1)[0] - ((int*)t2)[0];
}
/*入度地图排序,第一列进行升序排序,如果第一列相等，则按照第二列进行降序排序*/
int compare_3(const void *t1, const void *t2) { 
	return ((int*)t1)[0] == ((int*)t2)[0] ? ((int*)t2)[1] - ((int*)t1)[1] : ((int*)t1)[0] - ((int*)t2)[0];
}

int myitoa(char* dest, int num)
{
	int n(0);
	if (num == 0) dest[n++] = '0';
	while (num != 0)
	{
		dest[n++] = (num % 10) + '0';
		num /= 10;
	}
	for (int i = 0; i < n / 2; ++i)
	{
		dest[i] = dest[i] ^ dest[n - i - 1];
		dest[n - i - 1] = dest[i] ^ dest[n - i - 1];
		dest[i] = dest[i] ^ dest[n - i - 1];
	}
	return n;
}

int myatoi(const char *nptr)
{
	int total(0);         /* current total */
	char c = *nptr++;	/* current char */
	while (c) {
		total = 10 * total + (c - '0');
		c = (char)*nptr++;
	}
	return total;   /* return result, negated if necessary */
}

void LoadTestData(string& testFile)
{
	FILE *stream = fopen(testFile.c_str(), "r");
#ifdef TEST
	if (stream == nullptr) {
		cout << "open result file error" << endl; 
		return; 
	}
#endif
	/*读入char数组，并缓存于oriData*/
	fseek(stream, 0L, SEEK_END);
	long size = ftell(stream);
	rewind(stream);
	char *oriData = (char*)malloc(size * sizeof(char));
	fread(oriData, size, 1, stream);

	char* temp = oriData;
	char* pre;
	int startid, endid, money, len, textrow(0);
	stringIndex.reserve(MAX_DATA_ROW);
	while (temp < oriData + size)
	{
		/*分割出startid，并将字符串缓存于strBufs中*/
		len = 0;
		pre = temp;
		while (*temp != ',') { temp++; len++; }
		*temp++ = '\0';
		startid = myatoi(pre);
		strBufs2[textrow].str = pre;
		strBufs2[textrow].len = len;
		stringIndex[startid] = textrow; //映射字符串地址下标

		/*分割出endid，并将字符串缓存于strBufs中*/
		len = 0;
		pre = temp;
		while (*temp != ',') { temp++; len++;}
		*temp++ = '\0';
		endid = myatoi(pre);

		/*分割出money，并存入allMoney*/
		pre = temp;
		while (*temp >= '0') { temp++; len++;}
		*temp++ = '\0';
		while(*temp < '0'){ temp++; }
		money = myatoi(pre);
		forwardData[textrow][0] = startid;
		forwardData[textrow][1] = endid;
		forwardData[textrow++][2] = money;
	}
	validDataRow = textrow;
#ifdef TEST
	fclose(stream);
	cout << "read " << textrow << "rows data" << endl;
	cout << "uint OK" << endl;
#endif
}
/*创建图*/
void CreateGraph()
{
	int cnt(0),startid;
	/*forwardData升序排序，对第一列升序排序 */
	qsort(forwardData, validDataRow, sizeof(forwardData[0]), compare_1);
	/*将startid节点ID映射为有序ID,从1开始逐渐递增*/
	tranIndex.reserve(validDataRow);
	for (int i = 0; i < validDataRow;) {
		startid = forwardData[i][0];
		tranIndex[startid] = ++cnt;
		//重新缓存字符串地址
		strBufs[cnt].str = strBufs2[stringIndex[startid]].str;
		strBufs[cnt].len = strBufs2[stringIndex[startid]].len;
		while (++i < validDataRow)
			if (forwardData[i - 1][0] != forwardData[i][0]) break;
	}
	validNodeNumber = cnt + 1;
	
	//更新forwardData为有序id
	for (int i = 0; i < validDataRow; ++i) {	
		if (tranIndex[forwardData[i][1]] != 0) {
			forwardData[i][0] = tranIndex[forwardData[i][0]];
			forwardData[i][1] = tranIndex[forwardData[i][1]];
		}
		else{
			forwardData[i][0] = -1; //-1代表该行无效
		}
	}
	//对forwardData表重新升序排序,优先级为第一列 > 第二列,已经排除出入度为0的节点
	qsort(forwardData, validDataRow, sizeof(forwardData[0]), compare_2);
	//生成inverseData
	cnt = 0;
	int i = -1;
	while(forwardData[++i][0]==-1){}
	int reverseDataRow = validDataRow - i;
	int forwardStartIndex = i;

	for (; i < validDataRow; ++i)
	{
		reverseData[cnt][0] = forwardData[i][1]; //endid
		reverseData[cnt][1] = forwardData[i][0];  //startid
		reverseData[cnt++][2] = forwardData[i][2];  //money
	}
	//对inverseData进行排序,优先级为第一列升序,然后第二列降序,已经排除出入度为0的节点
	qsort(reverseData, reverseDataRow, sizeof(reverseData[0]), compare_3);

	/*统计各边起始位置  x1<= y <x2 */
	outEdgeIndex = (int(*)[2])malloc(sizeof(int)*validNodeNumber * 2);
	inEdgeIndex = (int(*)[2])malloc(sizeof(int)*validNodeNumber * 2);
	fill(outEdgeIndex[0], outEdgeIndex[0] +(validNodeNumber * 2), -1);
	fill(inEdgeIndex[0], inEdgeIndex[0] + (validNodeNumber * 2), -1);

	int temp;
	for (int i = forwardStartIndex; i < validDataRow;)
	{
		temp = i;
		outEdgeIndex[forwardData[temp][0]][0] = i;
		while (++i < validDataRow) 
			if (forwardData[i - 1][0] != forwardData[i][0]) break;
		outEdgeIndex[forwardData[temp][0]][1] = i;
	}

	for (int i = 0; i < reverseDataRow;)
	{
		temp = i;
		inEdgeIndex[reverseData[temp][0]][0] = i;
		while (++i < reverseDataRow)
			if (reverseData[i - 1][0] != reverseData[i][0]) break;
		inEdgeIndex[reverseData[temp][0]][1] = i;
	}
#ifdef TEST
	cout << "After deleting imposible nodes,the total nodes has: " << validNodeNumber << endl;
	cout << "creat map OK..." << endl;
#endif
}
/*查找环*/
void findCircle(int threadId)
{
#ifdef CPU_AFFINITY
	pid_t pid = syscall(SYS_gettid);
	pthread_t tid = pthread_self();
	int cpus = sysconf(_SC_NPROCESSORS_ONLN);

	cpu_set_t mask;
	cpu_set_t get;
	CPU_ZERO(&mask);

	CPU_SET(threadId%cpus, &mask);
	if (pthread_setaffinity_np(tid, sizeof(mask), &mask) != 0)
	{
		cout << "set (pthread_setaffinity_np fasle" << endl;
	}
#endif // CPU_AFFINITY
	
	char* record = (char*)malloc(sizeof(char)*validNodeNumber);
	int* lastMoney = (int*)malloc(sizeof(int)*validNodeNumber);

	int index(0),it(threadId+1);
	//int index(0), it(0);

	//遍历顶点节点
	while(it < validNodeNumber)
	{
		fill(record, record + validNodeNumber, 0);
		RecordDFSIn(it, record, lastMoney);//预搜索3层入度
		PDFS(it, record, lastMoney, threadId);
		it = CORE_NUM * ++index + threadId + 1;
	}
#ifdef TEST
	cout << "threadId: "<< threadId << "has finshed..." << endl;
#endif // TEST
}
//only search three levels
void RecordDFSIn(int first, char* record, int* lastMoney)
{
	int depth1, depth2, depth3;
	int money1, money2, money3;
	int i1, i2, i3;

	record[first] |= VISITED;
	for (i1=inEdgeIndex[first][0]; i1 < inEdgeIndex[first][1]; ++i1)	//遍历第一层的入度边
	{
		depth1 = reverseData[i1][1];

		if (depth1 < first) break;
		money1 = reverseData[i1][2];
		record[depth1] |= DISTANCE_1 | VISITED; //标记逆1层节点距离为1且已访问
		lastMoney[depth1] = money1;	//存放最后一段金额

		for (i2 = inEdgeIndex[depth1][0]; i2 < inEdgeIndex[depth1][1]; ++i2)	//遍历第二层的出度边
		{
			depth2 = reverseData[i2][1];
			if (depth2 < first) break;
			if (record[depth2] & VISITED) continue; //已访问，跳过
			money2 = reverseData[i2][2];

			if ((int64_t)5 * money1 < money2 || money1 >(int64_t)3 * money2) continue;
			record[depth2] |= DISTANCE_2 | VISITED;//标记逆2层节点距离为2且已访问

			for (i3 = inEdgeIndex[depth2][0]; i3 < inEdgeIndex[depth2][1]; ++i3)	//遍历第三层的出度边
			{
				depth3 = reverseData[i3][1];
				if (depth3 < first) break;
				money3 = reverseData[i3][2];
				if (depth1 == depth3|| (int64_t)5 * money2 < money3 || money2 >(int64_t)3 * money3) continue;
				record[depth3] |= DISTANCE_3; //标记逆3层节点距离为3
			}
			record[depth2] &= UN_VISITED; //标记逆2层节点为未访问
		}
		record[depth1] &= UN_VISITED; //标记逆1层节点为未访问
	}
	record[first] &= UN_VISITED; //标记逆0层节点为未访问
}

void PDFS(int first, char* record, int* lastMoney,int threadId)
{
	int depth1, depth2, depth3, depth4, depth5, depth6;
	int money1, money2, money3, money4, money5, money6;
	int64_t money15;
	int i1, i2, i3, i4, i5, i6;
	int _record;

	record[first] |= VISITED;	//标记第0层节点为访问
	findMinIndex(first,first,i1);
	for (; i1 < outEdgeIndex[first][1]; ++i1)	//遍历第2个节点
	{
		depth1 = forwardData[i1][1];	
		money1 = forwardData[i1][2];
		money15 = (int64_t)5 * money1;
		record[depth1] |= VISITED;	//标记第1层节点为访问

		findMinIndex(depth1, first, i2);
		for (; i2 < outEdgeIndex[depth1][1]; ++i2)	//遍历第3个节点
		{
			depth2 = forwardData[i2][1];
			if (record[depth2] & VISITED) continue;	//如果第二层节点已访问，跳过
			money2 = forwardData[i2][2];
			//判断是否满足金额约束
			if ((int64_t)5 * money2 < money1 || money2 >(int64_t)3 * money1) continue;
			if (record[depth2] & DISTANCE_1) {//找到第三环
				_record = lastMoney[depth2];
				if ((int64_t)5 * _record >= money2 && _record <= (int64_t)3 * money2 && \
					money15 >= _record && money1 <= (int64_t)3 * _record) {
					save3Cirecle(first, depth1, depth2, threadId);
				}
			}
			record[depth2] |= VISITED;	//标记第2层节点为访问
			findMinIndex(depth2, first, i3);
			for (; i3 < outEdgeIndex[depth2][1]; ++i3)	//遍历第4个节点
			{
				depth3 = forwardData[i3][1];
				if (record[depth3] & VISITED) continue;	//如果第三层节点已访问，跳过
				money3 = forwardData[i3][2];
				//判断是否满足金额约束
				if ((int64_t)5 * money3 < money2 || money3 >(int64_t)3 * money2) continue;	//不满足金额约束，跳过
				if (record[depth3] & DISTANCE_1) {//找到第四环
					_record = lastMoney[depth3];
					if ((int64_t)5 * _record >= money3 && _record <= (int64_t)3 * money3 && \
						money15 >= _record && money1 <= (int64_t)3 * _record) {
						save4Cirecle(first, depth1, depth2, depth3, threadId);
					}
				}
				record[depth3] |= VISITED;	//标记第3层节点为访问
				findMinIndex(depth3, first, i4);
				for (; i4 < outEdgeIndex[depth3][1]; ++i4)	//遍历第5个节点
				{
					depth4 = forwardData[i4][1]; //取出节点
					money4 = forwardData[i4][2];//取出金额

					//判断是否满足金额  是否已被访问  是否具有第五层距离标记
					if ((record[depth4] & VISITED)|| !(record[depth4] & FIVE_LEVEL)||(int64_t)5 * money4 < money3 || money4 >(int64_t)3 * money3) continue; // 金额约束

					if (record[depth4] & DISTANCE_1) {//找到第五环
						_record = lastMoney[depth4];
						//金额约束
						if ((int64_t)5 * _record >= money4 && _record <= (int64_t)3 * money4 && \
							money15 >= _record && money1 <= (int64_t)3 * _record) {
							save5Cirecle(first, depth1, depth2, depth3, depth4, threadId);
						}
					}
					record[depth4] |= VISITED;

					findMinIndex(depth4, first, i5);
					for (; i5 < outEdgeIndex[depth4][1]; ++i5)	//遍历第6个节点
					{
						depth5 = forwardData[i5][1];//取出节点
						money5 = forwardData[i5][2];//取出金额
						//判断是否满足金额  是否已被访问  是否具有第六层距离标记
						if ((record[depth5] & VISITED)|| !(record[depth5] & SIX_LEVEL)||(int64_t)5 * money5 < money4 || money5 >(int64_t)3 * money4) continue;

						if (record[depth5] & DISTANCE_1) {//找到第六环
							_record = lastMoney[depth5];
							if ((int64_t)5 * _record >= money5 && _record <= (int64_t)3 * money5 && \
								money15 >= _record && money1 <= (int64_t)3 * _record) {
								save6Cirecle(first, depth1, depth2, depth3, depth4, depth5, threadId);
							}
						}
						record[depth5] |= VISITED;

						findMinIndex(depth5, first, i6);
						for (; i6 < outEdgeIndex[depth5][1]; ++i6)	//遍历第7个节点
						{
							depth6 = forwardData[i6][1];
							money6 = forwardData[i6][2];

							//判断是否满足金额  是否已被访问  是否具有第六层距离标记
							if ((record[depth6] & VISITED) || !(record[depth6] & DISTANCE_1) || (int64_t)5 * money6 < money5 || money6 >(int64_t)3 * money5) continue;
								
							//找到第七环
							_record = lastMoney[depth6];
							if ((int64_t)5 * _record >= money6 && _record <= (int64_t)3 * money6 && \
								money15 >= _record && money1 <= (int64_t)3 * _record) {
								save7Cirecle(first, depth1, depth2, depth3, depth4, depth5, depth6, threadId);
							}
						}
						record[depth5] &= UN_VISITED; //标记第5层节点为未访问
					}
					record[depth4] &= UN_VISITED; //标记第4层节点为未访问
				}
				record[depth3] &= UN_VISITED; //标记第3层节点为未访问
			}
			record[depth2] &= UN_VISITED; //标记第2层节点为未访问
		}
		record[depth1] &= UN_VISITED; //标记第1层节点为未访问
	}
	//标记第0层节点为未访问 可省略
}


/*保存结果文档*/
void SaveResultData(string& resultFile)
{
	long long resultDataSize(0);
	int i, j, k(0), totalNum(0), fd, startWrite, endWrite, threadIndex;
	int hasWrite[CORE_NUM][5] = { 0 };//CORE_NUM个线程 5个结果集 记录每个线程已经输出的字节数
	strBuf Buff;

	fd = open(resultFile.c_str(), O_RDWR | O_CREAT | O_TRUNC, 00777);
	char totalNumBuf[10];
	//计算总环数totalNum，存储总字节数resultDataSize
	for (i = 0; i < CORE_NUM; ++i)
	{
		RESULT& _result = Result[i];
		resultDataSize += Result[i].size;
		totalNum += _result.rowCnt[0] / 3 + _result.rowCnt[1] / 4 + _result.rowCnt[2] / 5 + _result.rowCnt[3] / 6 + _result.rowCnt[4] / 7;
	}
	//将总环数转换为char，缓存totalNumBuf
	cout << "total circles: " << totalNum << endl;
	totalNum = myitoa(totalNumBuf, totalNum);
	totalNumBuf[totalNum++] = '\n';
	resultDataSize += totalNum;

	//写入总环数，并进行文件扩容操作
	write(fd, totalNumBuf, totalNum);
	lseek(fd, resultDataSize - 1, SEEK_SET);
	write(fd, " ", 1);
	char* ans = (char*)mmap(NULL, resultDataSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	close(fd);

	resultDataSize = totalNum;
	//交叉输出文件
	validNodeNumber--;//真实节点个数
	//len==3
	for (i = 0; i < validNodeNumber; ++i) {
		threadIndex = i % CORE_NUM;
		startWrite = hasWrite[threadIndex][0];
		hasWrite[threadIndex][0] += Result[threadIndex].circleCnt[0][i+1] * 3;
		endWrite = hasWrite[threadIndex][0];
		auto& _result = Result[threadIndex].result3;
		for (j = startWrite; j < endWrite; j += 3) {
			__builtin_prefetch(&_result[j] + 2, 0);
			Buff = strBufs[_result[j]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);	
			resultDataSize += Buff.len;
			ans[resultDataSize++] = ',';

			Buff = strBufs[_result[j + 1]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = ',';

			Buff = strBufs[_result[j + 2]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = '\n';
		}
	}
	//len==4
	for (i = 0; i < validNodeNumber; ++i) {
		threadIndex = i % CORE_NUM;
		startWrite = hasWrite[threadIndex][1];
		hasWrite[threadIndex][1] += Result[threadIndex].circleCnt[1][i+1] * 4;
		endWrite = hasWrite[threadIndex][1];

		auto& _result = Result[threadIndex].result4;
		for (j = startWrite; j < endWrite; j += 4) {
			__builtin_prefetch(&_result[j] + 3, 0);
			Buff = strBufs[_result[j]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = ',';

			Buff = strBufs[_result[j + 1]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = ',';

			Buff = strBufs[_result[j + 2]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = ',';

			Buff = strBufs[_result[j + 3]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = '\n';
		}
	}
	//len==5
	for (i = 0; i < validNodeNumber; ++i) {
		threadIndex = i % CORE_NUM;
		startWrite = hasWrite[threadIndex][2];
		hasWrite[threadIndex][2] += Result[threadIndex].circleCnt[2][i+1] * 5;
		endWrite = hasWrite[threadIndex][2];

		auto& _result = Result[threadIndex].result5;
		for (j = startWrite; j < endWrite; j += 5) {
			__builtin_prefetch(&_result[j] + 4, 0);
			Buff = strBufs[_result[j]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = ',';

			Buff = strBufs[_result[j + 1]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = ',';

			Buff = strBufs[_result[j + 2]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = ',';

			Buff = strBufs[_result[j + 3]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = ',';

			Buff = strBufs[_result[j + 4]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = '\n';
		}
	}
	//len==6
	for (i = 0; i < validNodeNumber; ++i) {
		threadIndex = i % CORE_NUM;
		startWrite = hasWrite[threadIndex][3];
		hasWrite[threadIndex][3] += Result[threadIndex].circleCnt[3][i+1] * 6;
		endWrite = hasWrite[threadIndex][3];

		auto& _result = Result[threadIndex].result6;
		for (j = startWrite; j < endWrite; j += 6) {
			__builtin_prefetch(&_result[j] + 5, 0);
			Buff = strBufs[_result[j]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = ',';

			Buff = strBufs[_result[j + 1]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = ',';

			Buff = strBufs[_result[j + 2]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = ',';

			Buff = strBufs[_result[j + 3]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = ',';

			Buff = strBufs[_result[j + 4]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = ',';

			Buff = strBufs[_result[j + 5]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = '\n';
		}
	}
	//len==7
	for (i = 0; i < validNodeNumber; ++i) {
		threadIndex = i % CORE_NUM;
		startWrite = hasWrite[threadIndex][4];
		hasWrite[threadIndex][4] += Result[threadIndex].circleCnt[4][i+1] * 7;
		endWrite = hasWrite[threadIndex][4];

		auto& _result = Result[threadIndex].result7;
		for (j = startWrite; j < endWrite; j += 7) {
			__builtin_prefetch(&_result[j] + 6, 0);
			Buff = strBufs[_result[j]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = ',';

			Buff = strBufs[_result[j + 1]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = ',';

			Buff = strBufs[_result[j + 2]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = ',';

			Buff = strBufs[_result[j + 3]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = ',';

			Buff = strBufs[_result[j + 4]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = ',';

			Buff = strBufs[_result[j + 5]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = ',';

			Buff = strBufs[_result[j + 6]];
			memcpy(ans + resultDataSize, Buff.str, Buff.len);
			resultDataSize += Buff.len;
			ans[resultDataSize++] = '\n';
		}
	}
#ifdef TEST
	cout << "save result.txt OK..." << endl;
#endif // TEST
#ifdef RUN
	exit(0);
#endif // RUN
}
int main(int argc, char *argv[])
{
#ifdef TEST
	steady_clock::time_point time_start = steady_clock::now();
	string testFile = "test_data.txt";
	string resultFile = "result.txt";
#endif // TEST
#ifdef RUN
	string testFile = "/data/test_data.txt";
	string resultFile = "/projects/student/result.txt";
#endif // RUN

	LoadTestData(testFile);

#ifdef TEST
	steady_clock::time_point time_end = steady_clock::now();
	duration<double> time_used = duration_cast<duration<double>>(time_end - time_start);
	cout << "LoadTestData time use:" << time_used.count() << "s" << endl;
	steady_clock::time_point time_start2 = steady_clock::now();
#endif // TEST

	CreateGraph();
	
#ifdef TEST
	time_end = steady_clock::now();
	time_used = duration_cast<duration<double>>(time_end - time_start2);
	cout << "CreateGraph time use:" << time_used.count() << "s" << endl;
	cout << CORE_NUM << " threads are running..." << endl;
	time_start2 = steady_clock::now();
#endif // TEST

	//找环
	thread **threads = new thread*[CORE_NUM];
	for (int i = 0; i < CORE_NUM; ++i)
	{
		threads[i] = new thread(&findCircle, i);
	}
	for (int i = 0; i < CORE_NUM; ++i)
	{
		threads[i]->join();
	}

#ifdef TEST
	time_end = steady_clock::now();
	time_used = duration_cast<duration<double>>(time_end - time_start2);
	cout << "findCircle time use:" << time_used.count() << "s" << endl;
	time_start2 = steady_clock::now();
#endif // TEST

	SaveResultData(resultFile);

#ifdef TEST
	time_end = steady_clock::now();
	time_used = duration_cast<duration<double>>(time_end - time_start2);
	cout << "SaveResultData time use:" << time_used.count() << "s" << endl;

	time_used = duration_cast<duration<double>>(time_end - time_start);
	cout << "all time use:" << time_used.count() << "s" << endl;
#endif // TEST
	return 0;
}


