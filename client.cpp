#include "tools/msgtool.h"
#include <iostream>

using namespace std;

int main(int argc, char *argv[])
{
	msg_queue_client client;
	long type=1;
	char tmp[80];
	while(true)
	{
		cout<<"Input Query: ";
		cin.get(tmp, 50);//readLine
		cin.get();//skipe line-breaker
		if(strcmp(tmp, "exit")==0) return 0;
		client.send_msg(type, tmp);
	}
	return 0;
}
