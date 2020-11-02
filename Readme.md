## README——181098273 王天诚

### 项目目录：

### ![目录](/Users/wangtiancheng/Documents/金融大数据/homework/hw6/pic/目录.png)







### 实验结果：

![result](/Users/wangtiancheng/Documents/金融大数据/homework/hw6/pic/result.png)



### 设计思路：

##### ==总体思路==：通过两次MapReduce实现

- 第一次MapReduce：对输入数据进行翻转，对每一个人，找到好友列表中有这个人的所有用户。以friends_list1,2中的数据为例，用户200/300/400/500/600的好友列表中都有用户100，因此第一次MapReduce后将数据输出成100  200,300,400,500,600这样的格式。
- 第二次MapReduce：观察第一次输出的结果，显然每一行除去第一个用户后面的用户两两之间都有共同好友，而他们的共同好友就是第一个用户。以上面的例子为例，可以生成<[200,300], [100]>,<[200,400], [100]>,<[200,500], [100]>,<[200,600], [100]>......这样的数据对。之后再reduce任务中按相同的key进行拼接即可。

==数据结构==：定义两个Job：job1, job2; 每个job对应一组mapper与reducer，前者的输出是后者的输入，第二个Job结束后删除中间的临时输出。



### 实现代码：

第一次MapReduce：

![image-20201102200243872](/Users/wangtiancheng/Library/Application Support/typora-user-images/image-20201102200243872.png)



第二次MapReduce：

![image-20201102200315292](/Users/wangtiancheng/Library/Application Support/typora-user-images/image-20201102200315292.png)





