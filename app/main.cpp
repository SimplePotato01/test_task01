/**
 * @file main.cpp
 * @brief Консольное приложение для демонстрации библиотеки логирования
 * 
 * Многопоточное приложение принимающее сообщения от пользователя
 * в консоли и записывающее их в журнал через отдельный поток-воркер.
 * Поддерживает интерактивные команды для изменения настроек логирования.
 * 
 * Возможности:
 * - Отправка сообщений с явным указанием уровня важности
 * - Изменение уровня важности по умолчанию "на лету"
 * - Потокобезопасная передача сообщений через очередь
 * - Корректное завершение работы с ожиданием записи всех сообщений
 * 
 * @see Logger
 * @see AsyncWriter
 */

#include "logger.hpp"
#include <iostream>
#include <thread>
#include <queue>
#include <condition_variable>
#include <atomic>
#include <sstream>
#include <algorithm>
#include <cctype>
#include <memory>
#include <map>

using logger::Logger;
using logger::Level;

/**
 * @namespace Анонимное пространство имён
 * @brief Внутренние компоненты приложения невидимые снаружи
 *
 * Содержит класс AsyncWriter и вспомогательные функции
 * используемые только в пределах main.cpp.
 */
namespace {
    /**
     * @struct LogMessage
     * @brief Сообщение для очереди логирования
     * 
     * Представляет одно сообщение помещаемое в очередь
     * для последующей обработки потоком-воркером.
     * Поддерживает специальное сообщение-маркер для
     * корректного завершения потока.
     */
    struct LogMessage {
        Level level; ///< Уровень важности сообщения
        std::string text; ///< Текст сообщения
        bool isShutdown = false; ///< Флаг завершения работы воркера

        /**
         * @brief Конструктор обычного сообщения
         * @param l Уровень важности
         * @param t Текст сообщения (перемещается, не копируется)
         */
        LogMessage(Level l, std::string t) : level(l), text(std::move(t)) {}
        /**
         * @brief Создать специальное сообщение для остановки воркера
         * @return LogMessage Сообщение с флагом isShutdown = true
         */
        static LogMessage shutdown() {
            LogMessage msg(Level::STATE3, "");
            msg.isShutdown = true;
            return msg;
        }
    };
    
    /**
     * @class AsyncWriter
     * @brief Асинхронный писатель в журнал
     *
     * Реализует паттерн "Producer-Consumer" с одним потоком-воркером.
     * Основной поток помещает сообщения в очередь воркер извлекает
     * их и записывает в журнал через Logger.
     *
     * Потокобезопасность обеспечивается мьютексом и условной переменной.
     *
     * @note Воркер запускается в конструкторе и останавливается
     *  в деструкторе или при явном вызове stop().
     */
    class AsyncWriter {
        public:
            /**
             * @brief Конструктор — запускает поток-воркер
             * 
             * Создаёт и запускает отдельный поток который будет
             * обрабатывать очередь сообщений.
             */
            AsyncWriter() : running_(true) {
                worker_ = std::thread(&AsyncWriter::processQueue, this);
            }

            /**
             * @brief Деструктор — гарантирует остановку воркера
             *
             * Вызывает stop() для корректного завершения потока
             * и освобождения ресурсов.
             */
            ~AsyncWriter() {
                stop();
            }

            /**
             * @brief Поместить сообщение в очередь на запись
             *
             * Потокобезопасно добавляет сообщение в очередь
             * и уведомляет поток-воркер о новых данных.
             *
             * @param level Уровень важности сообщения
             * @param text Текст сообщения
             *
             * @note Если писатель уже остановлен, сообщение игнорируется.
             */
            void push(Level level, const std::string& text) {
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (!running_) return; // Не принимаем новые сообщения при завершении
                    queue_.emplace(level, text);
                }
                cv_.notify_one(); // Будим воркер (вне блокировки для эффективности)
            }

            /**
             * @brief Остановить писатель и дождаться завершения воркера
             *
             * Помещает в очередь специальное сообщение-маркер,
             * которое заставит воркер завершиться после обработки
             * всех предыдущих сообщений.
             *
             * @note Безопасен для многократного вызова.
             */
            void stop() {
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (!running_) return; // Уже остановлен
                    running_ = false;
                    queue_.push(LogMessage::shutdown()); // Сигнал к завершению
                }
                cv_.notify_all(); // Будим воркер для обработки shutdown-сообщения
                // Дожидаемся завершения потока если он ещё работает
                if (worker_.joinable())
                    worker_.join();
            }

        private:
            /**
             * @brief Основной цикл потока-воркера
             * 
             * Бесконечно ожидает сообщения в очереди, извлекает их
             * и передаёт в Logger для записи. Завершается при получении
             * сообщения с флагом isShutdown.
             * 
             * @note Выполняется в отдельном потоке.
             */
            void processQueue() {
                while (true) {
                    std::unique_lock<std::mutex> lock(mutex_);
                    // Ждём, пока очередь не станет непустой.
                    // Лямбда проверяет условие при пробуждении,
                    // защищая от ложных пробуждений (spurious wakeups).
                    // Я плох в лямбде.
                    cv_.wait(lock, [this] { return !queue_.empty(); });

                    // Извлекаем сообщение из очереди
                    LogMessage msg = std::move(queue_.front());
                    queue_.pop();
                    // Разблокируем мьютекс перед длительной операцией записи
                    // чтобы не блокировать producer'ов
                    lock.unlock();
                    // Проверяем, не сигнал ли это к завершению
                    if (msg.isShutdown) break;

                    // Записываем сообщение в журнал.
                    // Если запись не удалась, выводим ошибку в stderr
                    Logger::instance().log(msg.level, msg.text);
                }
            }

            std::queue<LogMessage> queue_; ///< Очередь сообщений на запись
            std::mutex mutex_; ///< Мьютекс для защиты очереди
            std::condition_variable cv_; ///< Условная переменная для уведомлений
            std::thread worker_; ///< Поток-воркер
            std::atomic<bool> running_; ///< Флаг работы (атомарный для безопасности)
    };

    /**
     * @brief Распарсить строку в уровень важности
     *
     * Выполняет регистронезависимое сравнение строки
     * с допустимыми названиями уровней.
     *
     * @param str Входная строка (может быть в любом регистре)
     * @param[out] out Результат парсинга (изменяется только при успехе)
     *
     * @return true Строка соответствует одному из уровней
     * @return false Строка не распознана как уровень важности
     *
     * @note Допустимые значения: "STATE1", "STATE2", "STATE3"
     *  (регистр не учитывается и нет варианта UNKNOWN).
     */
    bool parseLevel(const std::string& str, Level& out) {
        std::string upper = str;
        std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
        if (upper == "STATE1") out = Level::STATE1;
        else if (upper == "STATE2") out = Level::STATE2;
        else if (upper == "STATE3") out = Level::STATE3;
        else return false;
        return true;
    }

    /**
     * @brief Вывести справку по использованию программы
     *
     * Показывает синтаксис командной строки и интерактивных команд.
     *
     * @param prog Имя исполняемого файла (обычно argv[0])
     */
    void printUsage(const std::string& prog) {
        std::cout << "Usage: " << prog << " <logfile> [default_level]\n"
            << "  logfile        Path to log file\n"
            << "  default_level STATE1 | STATE2 | STATE3 (default: STATE3)\n"
            << "\nInteractive commands:\n"
            << "  <text>                Log with default level\n"
            << "  <LEVEL> <text>        Log with specified level\n"
            << "  setlevel <LEVEL>      Change default level\n"
            << "  quit / q              Exit program\n";
    }

} // anonymous namespace

/**
 * @brief Точка входа в приложение
 *
 * Парсит аргументы командной строки, инициализирует логгер
 * и запускает интерактивный цикл обработки пользовательских команд.
 *
 * @param argc Количество аргументов командной строки
 * @param argv Массив строк аргументов
 *
 * @return int Код возврата:
 *         - 0: успешное завершение
 *         - 1: ошибка инициализации логгера
 *
 * Примеры запуска:
 * @code
 * ./logger_app app.log
 * ./logger_app /var/log/myapp.log STATE1
 * @endcode
 */
int main(int argc, char* argv[]) {
    // Значения по умолчанию
    std::string logFile = "app.log";
    Level defaultLevel = Level::STATE3;

    // Парсинг аргументов командной строки.
    // Стратегия: всё, что не флаг, считаем либо уровнем, либо путём к файлу.
    // Уровень пытаемся распарсить в первую очередь, остальное — путь к файлу.
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        // Обработка флага помощи
        if (arg == "-h" || arg == "--help") {
            printUsage(argv[0]);
            return 0;
        }
        // Если аргумент не начинается с '-' - считаем его именем файла или уровнем
        if (arg[0] != '-') {
            // Проверяем может ли это быть уровнем
            Level parsed;
            if (parseLevel(arg, parsed)) {
                defaultLevel = parsed;
            } else {
                logFile = arg;
            }
        }
    }

    // Инициализация логгера
    if (!Logger::instance().init(logFile, defaultLevel)) {
        std::cerr << "Failed to initialize logger.\n";
        return 1;
    }

    // Создаём асинхронный писатель (запускает отдельный поток)
    auto writer = std::make_unique<AsyncWriter>();

    // Приветственное сообщение и подсказки
    std::cout << "Logger started. Type 'help' for commands, 'quit' to exit.\n";
    std::cout << "Log file: " << logFile << "\n";

    // Главный цикл обработки пользовательского ввода
    std::string line;
    while (std::getline(std::cin, line)) {
        // Пропускаем пустые строки
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string cmd;
        iss >> cmd;
        // Приводим команду к нижнему регистру для удобства
        std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::tolower);

        if (cmd == "quit" || cmd == "q") {
            break;
        }
        if (cmd == "help") {
            printUsage(argv[0]);
            continue;
        }
        if (cmd == "setlevel") {
            std::string lvl;
            if (iss >> lvl) {
                Level newLevel;
                if (parseLevel(lvl, newLevel)) {
                    Logger::instance().setDefaultLevel(newLevel);
                    std::cout << "Default level changed to " << lvl << "\n";
                } else {
                    std::cout << "Invalid level.\n";
                }
            }
            continue;
        }

        // Обработка сообщений с явным указанием уровня
        Level msgLevel;
        if (parseLevel(cmd, msgLevel)) {
            // Первое слово — уровень, остальное — сообщение
            std::string message;
            std::getline(iss >> std::ws, message); // >> std::ws пропускает пробелы. читай man
            if (message.empty()) {
                std::cout << "Error: missing message after level.\n";
                continue;
            }
            writer->push(msgLevel, message);
        } else {
            writer->push(Logger::instance().getDefaultLevel(), line);
        }
        std::cout << "Queued\n";
    }

    // Корректное завершение работы
    std::cout << "Shutting down...\n";
    writer.reset();

    return 0;
}
