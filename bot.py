
import sqlite3
import logging
import configparser

import dateparser
from telegram.ext import Filters
from telegram.ext import Updater
from telegram.ext import MessageHandler


class ReminderBot(object):

    def __init__(self, config_file_path):
        self.connection = None
        self.job_queue = None
        config = configparser.ConfigParser()
        config.read(config_file_path)
        config_main = config['main']
        self.bot_token = config_main['bot_token']
        self.proxy_url = config_main['proxy_url']
        self.init_database(config_main['database'])

    def _callback_send_message(self, bot, job):
        """Отпавляет сообщение и помечает в БД как отправленное"""
        job_id, chat_id, text = job.context
        bot.send_message(chat_id=chat_id, text=text)
        cursor = self.connection.cursor()
        cursor.execute('update messages set sent=true where id=?', [job_id])
        self.connection.commit()

    def _add_job_to_queue(self, ordervalue, job_id, chat_id, text):
        """Добавляет сообщение в очередь на отправку в указанное время"""
        self.job_queue.run_once(callback=self._callback_send_message,
                                when=ordervalue,
                                context=(job_id, chat_id, text))

    def _add_message(self, ordervalue, chat_id, text):
        """Добавляет сообщение на хранение в БД и вызвает добавление в очередь на отправку"""
        cursor = self.connection.cursor()
        cursor.execute('insert into messages (chat_id, ordervalue, text, sent) values (?, ?, ?, ?)',
                       [chat_id, ordervalue, text, False])
        job_id = cursor.lastrowid
        self.connection.commit()
        self._add_job_to_queue(ordervalue, job_id, chat_id, text)

    def _receive_text(self, bot, update):
        """Обрабатывает полученные сообщения на предмет соответствия шаблону"""
        lines = update.message.text.split('\n')  # Разбиваем сообщения на строки
        if len(lines) < 2:
            bot.send_message(chat_id=update.message.chat_id,
                             text=('В сообщении должно быть не меньше двух строк, '
                                   'а в последней строке должно быть вермя напоминания.'))
            return
        last_line = lines[-1]  # Получаем последнюю строку
        timestamp = dateparser.parse(last_line, languages=['ru', 'en'])  # Парсим последнюю строку на предмет даты
        if not timestamp:
            bot.send_message(chat_id=update.message.chat_id,
                             text='В последней строке должно быть время напоминания в правильном формате.')
            return
        text = '\n'.join(lines[:-1])
        bot.send_message(chat_id=update.message.chat_id,
                         text='Напоминание будет выведено {}'.format(last_line))
        self._add_message(timestamp, update.message.chat_id, text)

    def init_database(self, database):
        self.connection = sqlite3.connect(database,
                                          check_same_thread=False,
                                          detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        self.connection.execute('''
        create table if not exists messages (
          id integer primary key autoincrement,
          chat_id bigint not null,
          ordervalue timestamp not null,
          text text not null,
          sent boolean not null
        )
        ''')
        self.connection.commit()

    def start_bot(self):
        # Прокси с сайта http://spys.one/
        request_kwargs = {
            'proxy_url': self.proxy_url,
            # Optional, if you need authentication:
            # 'username': 'PROXY_USER',
            # 'password': 'PROXY_PASS',
        }
        updater = Updater(token=self.bot_token, request_kwargs=request_kwargs)
        dispatcher = updater.dispatcher
        self.job_queue = updater.job_queue
        receive_handler = MessageHandler(Filters.text, self._receive_text)
        dispatcher.add_handler(receive_handler)
        updater.start_polling()
        self._load_jobs()

    def _load_jobs(self):
        cursor = self.connection.cursor()
        cursor.execute('select id, chat_id, ordervalue, text from messages where sent=false order by ordervalue')
        for job_id, chat_id, ordervalue, text in cursor:
            self._add_job_to_queue(ordervalue, job_id, chat_id, text)


def main(config_file_path):
    bot = ReminderBot(config_file_path)
    bot.start_bot()


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO)
    main('config.ini')