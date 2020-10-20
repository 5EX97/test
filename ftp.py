#!/usr/bin/python
# -*- coding: UTF-8 -*-

from ftplib import FTP
import os
import time,datetime,json
import socket
import requests
import concurrent.futures


class FTPP:
    def __init__(self, host, port=21, devid='windows', username='', password=''):
        self.host = host
        self.port = port
        self.ftp = FTP()
        self.username = username
        self.password = password
        self.devid = devid
        self.url = 'https://httpbin.org/post'

    def login(self):
        # 设置连接超时时间为30
        try:
            timeout = 30
            socket.setdefaulttimeout(timeout)
            print('开始尝试连接到 %s' % self.host)
            self.ftp.connect(self.host, self.port)
            print('成功连接到 %s' % self.host)

            print('开始尝试登录到 %s' % self.host)
            self.ftp.login(self.username, self.password)
            print('成功登录到 %s' % self.host)

            self.ftp.encoding = 'utf-8'
            print(self.ftp.welcome)
            return True
        except Exception as err:
            print("FTP 连接或登录失败，请检查网络，错误描述为：%s" % err)
            return False

    def check_path(self, target_path):
        self.ftp.cwd('~')                       # 切换到服务器根目录，设为base_dir
        base_dir = self.ftp.pwd().encode('ISO-8859-1').decode('utf-8')
        temp_path = base_dir + target_path
        # target_path目标目录路径
        # 检查服务器相应目录路径是否存在,不存在则创建相应路径
        try:
            # 尝试切换到相应文件目录
            self.ftp.cwd(temp_path)
        except Exception as e1:
            # 不存在该目录
            part_dir = target_path.split('/')   # 分割目标目录路径
            for p in part_dir[1:]:              # 循环切入部分目录路径，若不存在则创建相应路径
                base_dir = base_dir + '/' + p
                try:
                    self.ftp.cwd(base_dir)
                except Exception as e2:
                    self.ftp.mkd(base_dir)

    def step_trans(self, localfile, remotefile, host, username, password, port=21):
        # 每个线程独立创建一个ftp连接，设置相应参数
        ftp = FTP()
        ftp.connect(host=host)
        ftp.login(username, password)
        ftp.encoding = 'utf-8'
        ftp.voidcmd('TYPE I')           #设置文件传输类型为二进制
        ftp.voidcmd('PASV')             # 设置服务器为被动接收模式
        localfile_size = os.path.getsize(localfile)
        # 尝试获取远程文件的大小
        try:
            remotefile_size = ftp.size(remotefile)
        except:
            remotefile_size = 0
        # APPE：ftp参数，表示将内容追加到文件的末尾
        cmd = 'APPE ' + remotefile
        while remotefile_size < localfile_size:
            try:
                with open(localfile, 'rb') as file:
                    file.seek(remotefile_size)
                    ftp.storbinary(cmd,file)
            except Exception as err:
                print('传输出现异常，尝试重新连接：' + repr(err))
                try:
                    ftp.connect(host=host)
                    ftp.login(username, password)
                    ftp.encoding = 'utf-8'
                    print(self.ftp.welcome)
                    ftp.voidcmd('TYPE I')  # 设置文件传输类型为二进制
                    ftp.voidcmd('PASV')    # 设置服务器为被动接收模式
                except Exception as e2:
                    print('重新连接失败，请检查网络：' + repr(e2))
                    return False
            remotefile_size = ftp.size(remotefile)
        # 最后判断是否传输完成，未完成则删除服务器文件，下次再传
        try:
            if ftp.size(remotefile) == localfile_size:
                # 传输结构化信息
                jsonpath = self.uptoInterface(localfile,remotefile)
                rpath = str(os.path.dirname(remotefile)) + '/' + str(os.path.split(jsonpath)[-1])
                with open(jsonpath,'rb') as f:
                    ftp.storbinary('APPE '+rpath,f)
                os.remove(jsonpath)
                # 删除本地文件
                os.remove(localfile)
                print(remotefile + '传输成功!')
            else:
                # 删除服务器文件
                ftp.delete(remotefile)
                print(remotefile + '传输失败!')
        except Exception as err:
            print('上传结构化信息出错：' + str(err))

    def allfile(self, localdir):
        """
        循环获取本地需上传的文件,并生成对应的远程文件路径,
        """
        list1 = os.listdir(localdir)
        local_file_list = []
        for i in range(0, len(list1)):
            temp = localdir + '/' + list1[i]
            if os.path.isdir(temp):
                l = self.allfile(temp)
                local_file_list.extend(l)
            if os.path.isfile(temp):
                local_file_list.append(temp)
        return local_file_list

    def mkdir(self, localdir, root_dir='root'):
        localfile = self.allfile(localdir)
        remote_file_list = []
        for l in localfile:
            fname = os.path.split(l)[-1]
            filecreatetime = time.localtime(os.stat(l).st_ctime)
            year = time.strftime('%Y', filecreatetime)
            month = time.strftime('%m', filecreatetime)
            day = time.strftime('%d', filecreatetime)
            remote_path = '/' + root_dir + '/' + year + '/' + month + '/' + day + '/' + self.devid
            self.check_path(remote_path)                           # 调用检查远程目录路径函数
            self.ftp.cwd('~')
            remote_file_list.append(self.ftp.pwd().encode('ISO-8859-1').decode('utf-8') + remote_path + '/' + fname)  # 组成完成的远程文件路径(包括文件名)
            # remote_file_list.append(list1[i])                        # 用于测试不建立太多级目录
        return remote_file_list


    def start(self, localdir, root_dir='root'):
        count = 0
        while self.allfile(localdir) and count<5:
            if self.login():
                local_file_list = self.allfile(localdir)
                remote_file_list = self.mkdir(localdir, root_dir)
                self.multiThread(5, local_file_list, remote_file_list)
            else:
                time.sleep(60)
                count += 1
                print(count)
        if count < 5 or not self.allfile(localdir):
            self.delete_dir(localdir)
            print('全部文件传输完成')
        # if self.login():
        #     local_file_list = self.allfile(localdir)
        #     remote_file_list = self.mkdir(localdir, root_dir)
        #     self.multiThread(5, local_file_list, remote_file_list)
        # self.delete_dir(localdir)
        # self.close()

    def multiThread(self, num, local_file, remote_file):
        """
        以下多线程池,池中线程数量max_worker,循环向线程池提交任务,
        最多同时有max_worker个线程在工作,若还有任务加入,新任务将等待,直到有线程空闲才执行.
        """
        future = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=num) as executor:
            for i in range(len(local_file)):
                args = (local_file[i], remote_file[i], self.host, self.username, self.password)
                # step_trans需接收多个参数,所以以可变参数(即:*参数)的形式传入
                executor.submit(self.step_trans, *args)
                # future.append(executor.submit(self.step_trans, *args))
            # for i in future:
            #     print(i.result())

    def uptoInterface(self, localfile, remotefile):
        list1 = []
        fname = os.path.split(localfile)[-1]
        list1.append(('rffilename', fname))
        list1.append(('rffilecreatetime', time.strftime('%Y/%m/%d %H:%M', time.localtime(os.stat(localfile).st_ctime))))
        list1.append(('rffilemodifytime', time.strftime('%Y/%m/%d %H:%M', time.localtime(os.stat(localfile).st_mtime))))
        list1.append(('rffileszie', os.path.getsize(localfile)))
        list1.append(('rffilepathdevice', localfile))
        list1.append(('rffilepathserver', remotefile))
        list1.append(('rffiletype', os.path.splitext(localfile)[-1]))
        list1.append(('rfvno', self.devid))

        dic = dict(list1)
        jsonname = str(os.path.dirname(localfile)) + '/' + fname + '.json'
        with open(jsonname, 'w', encoding='utf-8') as f:
            json.dump(dic, f, ensure_ascii=False)
        return jsonname
        # header = {'Content-Type': "application/json", "charset": "utf-8"}
        # return json.dumps(dic)
        # r = requests.post(url=self.url, data=json.dumps(dic), headers=header)
        # print(r.status_code)
        # print(r.text)

    def delete_dir(self,localdir):
        # 删除空的文件夹
        list1 = os.listdir(localdir)
        for i in range(0,len(list1)):
            path = localdir + '/' + list1[i]
            if os.path.isdir(path):
                self.delete_dir(path)
                if not os.listdir(path):
                    os.rmdir(path)
            else:
                pass

    def close(self):
        # 通知服务器关闭整个ftp连接
        try:
            self.ftp.quit()
        except Exception as err:
            print('退出错误：' + str(err))


if __name__ == "__main__":
    h1 = '192.168.56.51'
    h2 = '192.168.56.3'
    h3 = '172.29.100.9'
    h4 = '192.168.56.38'
    h5 = '172.26.214.34'
    my_ftp = FTPP(host=h3, username='123', password='')
    my_ftp.start(localdir='D:/aaaaaaaaaa', root_dir='root')