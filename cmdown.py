import sqlite3
import http.client
import json
import os


book_server = "cdn.cmanuf.com"


class BookDownloader(object):

    def __init__(self, isbn, edition, file_name, file_path, chap_num=-1):
        self.isbn = isbn
        self.edition = edition
        self.file_path = file_path
        self.file_name = file_name
        self.chap_num = chap_num
        self.server_index = 0
        self.file_size = 0
        self.file_block_num = 0
        self.success = False

        self.response_data = b''

        # create db
        db_file_name = 'cache/{}_{}.db'.format(self.isbn, self.chap_num)
        if not os.path.exists(db_file_name):

            # search server for pdf file
            for i in range(0, 10):
                self.server_index = i
                response = self.download_block(0)

                if response is None:
                    continue

                # 206 success
                if response.status == 206:
                    self.success = True
                    print("pdf file found server id: ", self.server_index)

                    # create database
                    self.database = sqlite3.connect(db_file_name)
                    self.db_cur = self.database.cursor()

                    # create tables
                    self.db_cur.execute(
                        '''CREATE TABLE book_info (
                            isbn               TEXT,
                            file_path          TEXT,
                            file_name          TEXT,
                            server_index       INTEGER DEFAULT (0),
                            file_size          INTEGER DEFAULT (0),
                            file_block_num     INTEGER DEFAULT (0),
                            download_block_num INTEGER DEFAULT (0),
                            PRIMARY KEY (
                                isbn
                            )
                        );'''
                    )
                    self.db_cur.execute(
                        '''CREATE TABLE book_data (
                            block   INTEGER,
                            status  BOOLEAN,
                            data    BLOB,
                            PRIMARY KEY (
                                block
                            )
                        );'''
                    )
                    self.database.commit()

                    # read file info
                    cont_range = response.headers.get('Content-Range')
                    cont_data = self.response_data
                    self.file_size = int(cont_range.split('/')[1])
                    self.file_block_num = self.file_size // 65535 + 1

                    # write file info to db
                    self.db_cur.execute(
                        '''INSERT into book_info
                            (isbn, file_path, file_name, server_index, file_size, file_block_num, download_block_num)
                            VALUES
                            (?, ?, ?, ?, ?, ?, 1);
                        ''',
                        (self.isbn, self.file_path, self.file_name, self.server_index, self.file_size, self.file_block_num)
                    )
                    self.db_cur.execute(
                        '''INSERT into book_data
                            (block, status, data)
                            VALUES
                            (?, ?, ?)
                        ''',
                        (0, 1, cont_data)
                    )
                    self.database.commit()
                    break
                else:
                    response.read()

        else :
            # connect to db
            self.database = sqlite3.connect(db_file_name)
            self.db_cur = self.database.cursor()

            # read file info from db
            self.db_cur.execute(
                'SELECT server_index, file_size, file_block_num FROM book_info WHERE isbn=:isbn;',
                {'isbn': self.isbn}
            )
            file_info = self.db_cur.fetchone()
            if file_info is None:
                print("数据库错误， 请删除后重试")
            self.server_index = file_info[0]
            self.file_size = file_info[1]
            self.file_block_num = file_info[2]


    def download_block(self, block_id):
        http_conn = http.client.HTTPConnection(book_server)
        f_start = block_id * 65536
        f_end = f_start + 65535
        file_range = 'bytes={s}-{e}'.format(s=f_start, e=f_end)
        book_header = {
            "Host": "cdn.cmanuf.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Accept-Encoding": "gzip, deflate",
            'Range': file_range,
            "Connection": "keep-alive",
            "Referer": "http://cdn.cmanuf.com/pdfReader/generic/build/pdf.worker.js"
        }

        if self.chap_num == -1:
            pdf_url = '/books/{sid}/{isbn}_{ed}/PDF/{isbn}_{ed}_2.pdf'.format(
                sid=self.server_index,
                isbn=self.isbn,
                ed=self.edition
            )
        else :
            pdf_url = '/books/{sid}/{isbn}_{ed}/Chapter/{isbn}_{ed}_{cp:0>2}.pdf'.format(
                sid=self.server_index,
                isbn=self.isbn,
                ed=self.edition,
                cp=self.chap_num
            )

        # get file_block from server
        try:
            http_conn.request(method='GET', url=pdf_url, headers=book_header)
            response = http_conn.getresponse()
            self.response_data = response.read()
            http_conn.close()
        except:
            response = None
            self.response_data = b''

        return response


    def download(self):
        self.db_cur.execute(
            'SELECT file_block_num, download_block_num FROM book_info WHERE isbn=:isbn LIMIT 1;',
            {'isbn': self.isbn}
        )
        file_info = self.db_cur.fetchone()
        if file_info is None:
            print("数据库错误， 请删除后重试")
        total_block_num = file_info[0]
        download_block_num = file_info[1]

        # 下载完成
        if download_block_num >= total_block_num:
            print('下载完成')
            return True

        # 下载未完成，继续下载
        else:
            for i in range(0, total_block_num):
                self.db_cur.execute(
                    'SELECT status FROM book_data WHERE block=:block LIMIT 1;',
                    {'block': i}
                )
                block_info = self.db_cur.fetchone()

                # 该块还没有成功下载
                if block_info is None:
                    response = self.download_block(i)
                    if response is None:
                        continue

                    if response.status == 206:
                        cont_data = self.response_data
                        download_block_num += 1
                        self.db_cur.execute(
                            '''INSERT into book_data
                                (block, status, data)
                                VALUES
                                (?, ?, ?)
                            ''',
                            (i, 1, cont_data)
                        )
                        self.db_cur.execute(
                            'UPDATE book_info set download_block_num=:num WHERE isbn=:isbn;',
                            {'num': download_block_num, 'isbn': self.isbn}
                        )
                        self.database.commit()
                        print('[{book_name}][{cb:>5d}/{tb:>5d}]: {pc:.2f}%'.format(
                            book_name=self.file_name,
                            cb=i+1,
                            tb=total_block_num,
                            pc=(100*download_block_num/total_block_num)
                            )
                        )
                else:
                    continue
            return True


    def output_pdf(self):
        self.db_cur.execute(
            'SELECT file_block_num, download_block_num FROM book_info WHERE isbn=:isbn LIMIT 1;',
            {'isbn': self.isbn}
        )
        file_info = self.db_cur.fetchone()
        total_block_num = file_info[0]
        download_block_num = file_info[1]

        if download_block_num < total_block_num:
            print('下载未完成[{}/{}]'.format(download_block_num, total_block_num))
            return False

        if self.file_path == '':
            fpath = 'download/' + self.file_name + '.pdf'
        else:
            if not os.path.exists('download/' + self.file_path):
                os.mkdir('download/' + self.file_path)
            fpath = 'download/' + self.file_path + '/' + self.file_name + '.pdf'

        print('正在完成pdf输出: {fp}'.format(fp=fpath))
        f = open(fpath, 'wb')
        for i in range(0, total_block_num):
            self.db_cur.execute(
                'SELECT data FROM book_data WHERE block=:block LIMIT 1;',
                {'block': i}
            )
            block_data = self.db_cur.fetchone()
            if block_data is None:
                print('数据库损坏或下载未完成')
                return False
            bin_data = block_data[0]
            f.write(bin_data)

        f.close()
        print('完成！')




def init_env():
    if not os.path.exists('cache'):
        os.mkdir('cache')

    if not os.path.exists('download'):
        os.mkdir('download')


def run_app():
    init_env()

    cfg_file = open('books.json')
    cfg = json.load(cfg_file)
    books = cfg.get('books')
    cfg_file.close()

    for book in books:
        if book.get('chapter_num') > 1:
            for chp_num in range(0, book.get('chapter_num')):
                book_download = BookDownloader(
                    book['isbn'],
                    book['edition'],
                    book['chapter_name'][chp_num],
                    book['book_name'],
                    chp_num
                )
                if book_download.download() == True:
                    book_download.output_pdf()
        else:
            book_download = BookDownloader(
                book['isbn'],
                book['edition'],
                book['book_name'],
                '',
                -1
            )
            if book_download.download() == True:
                book_download.output_pdf()


if __name__ == '__main__':
    run_app()