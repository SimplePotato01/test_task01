/**
 * @file logger.cpp
 * @brief Реализация библиотеки логирования
 * 
 * Содержит реализацию всех методов класса Logger
 * и вспомогательных функций для работы с временными
 * метками и форматированием сообщений журнала.
 * 
 * @see logger.hpp
 */

#include "logger.hpp"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <ctime>
#include <filesystem>  // C++17 std::filesystem — часть STL

namespace logger {
    /**
     * Публичные методы
     */

    Logger& Logger::instance() {
        // Идиома Мейерса: гарантирует потокобезопасную инициализацию
        // единственного экземпляра в C++11 и новее
        static Logger logger;
        return logger;
    }

    Logger::~Logger() {
        // Автоматическое освобождение ресурсов:
        // закрываем файл если он был открыт
        if (file_.is_open()) file_.close();
    }

    bool Logger::init(const std::string& filename, Level default_level) {
        // Предотвращаем повторную инициализацию:
        // acquire для синхронизации с release в конце успешного init
        if (initialized_.load(std::memory_order_acquire)) return false;

        // Создаём директории для файла журнала, если они не существуют.
        // std::filesystem — часть C++17 STL, не требует сторонних библиотек.
        std::filesystem::path filePath(filename);
        if (filePath.has_parent_path()) {
            std::error_code ec;
            std::filesystem::create_directories(filePath.parent_path(), ec);
            if (ec) {
                std::cerr << "Failed to create directory: " << filePath.parent_path() 
                    << " (" << ec.message() << ")" << std::endl;
                return false;
            }
        }
        // Открываем файл в режиме добавления (append).
        // Если файл не существует, он будет создан.
        file_.open(filename, std::ios::app);
        if (!file_.is_open()) {
            std::cerr << "Failed to open log file: " << filename << std::endl;
            return false;
        }

        // Сохраняем параметры инициализации
        filename_ = filename;
        defaultLevel_.store(default_level, std::memory_order_release);
        initialized_.store(true, std::memory_order_release);

        // Записываем стартовое сообщение с максимальным уровнем детализации
        log(Level::STATE3, "Logger started. Default level: " + levelToString(default_level));
        return true;
    }

    void Logger::setDefaultLevel(Level level) noexcept {
        // Проверяем, что логгер был инициализирован
        if (initialized_.load(std::memory_order_acquire)) {
            // Атомарно меняем уровень и получаем старое значение
            // memory_order_acq_rel для полной синхронизации операции обмена
            auto old = defaultLevel_.exchange(level, std::memory_order_acq_rel);
            // Логируем факт изменения уровня (всегда с STATE3 чтобы не зависеть от фильтра)
            log(Level::STATE3, "Default level changed from " + levelToString(old) + " to " + levelToString(level));
        } else {
            return;
        }
    }   

    Level Logger::getDefaultLevel() const noexcept {
        // acquire для получения актуального значения записанного с release
        return defaultLevel_.load(std::memory_order_acquire);
    }

    bool Logger::log(Level level, const std::string& message) {
        // Проверка инициализации логгера
        if (!initialized_.load(std::memory_order_acquire)) return false;
        // Фильтрация сообщений по уровню важности
        if (!shouldLog(level)) return true; // Не ошибка, просто пропускаем сообщение

        // Формируем строку записи: [время] [уровень] сообщение
        std::ostringstream entry;
        entry << "[" << timestamp() << "] "
            << "[" << levelToString(level) << "] "
            << message << "\n";

        // Блокируем мьютекс на время записи для потокобезопасности.
        // std::lock_guard автоматически освободит мьютекс при выходе из блока.
        std::lock_guard<std::mutex> lock(writeMutex_);
        try {
            file_ << entry.str();
            file_.flush(); // Принудительно сбрасываем буфер на диск из-за специфики
                           // работы ОС с ФС
            if (file_.fail()) {
                std::cerr << "Write failed" << std::endl;
                return false;
            }
        } catch (const std::exception& e) {
            std::cerr << "Write exception: " << e.what() << std::endl;
            return false;
        }
        return true;
    }

    // Делегируем вызов основному методу с уровнем по умолчанию
    bool Logger::log(const std::string& message) {
        return log(getDefaultLevel(), message);
    }

    /**
      * Приватные методы
      */
    bool Logger::shouldLog(Level level) const noexcept {
        /**
         * Логика фильтрации:
         * 
         * Уровни кодируются как:
         *   STATE1   = 0 (наивысшая важность)
         *   STATE2   = 1 (средняя важность)
         *   STATE3   = 2 (низшая важность)
         * 
         * Сообщение записывается если его важность
         * не ниже пороговой (значение <= порога).
         * 
         * @note Меньшее числовое значение = большая важность
         */
        return static_cast<int>(level) <= static_cast<int>(getDefaultLevel());
    }

    std::string Logger::levelToString(Level level) {
        // Преобразование enum в человекочитаемую строку для вывода в журнал
        switch (level) {
            case Level::STATE1:   return "STATE1";
            case Level::STATE2:   return "STATE2";
            case Level::STATE3:   return "STATE3";
        }
        return "UNKNOWN";
    }

    std::string Logger::timestamp() {
        using namespace std::chrono;
        // Получаем текущее системное время
        auto now = system_clock::now();
        // Извлекаем миллисекунды (остаток от деления на 1000 мс)
        auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
        // Конвертируем в time_t для работы с календарными функциями
        auto timer = system_clock::to_time_t(now);

        // Используем потокобезопасную localtime_r вместо localtime
        // (POSIX-функция, доступна в Linux-окружении, удов-ет условию задания об STL)
        std::tm bt;
        localtime_r(&timer, &bt);
        // Форматируем строку: YYYY-MM-DD HH:MM:SS.mmm
        std::ostringstream oss;
        oss << std::put_time(&bt, "%Y-%m-%d %H:%M:%S");
        oss << '.' << std::setfill('0') << std::setw(3) << ms.count();
        return oss.str();
    }
} // namespace logger
