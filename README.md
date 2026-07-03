📌 Service Booking Microservices
Система микросервисов для управления бронированием услуг, разработанная на Go с использованием архитектуры Event-Driven и Apache Kafka для асинхронного взаимодействия между сервисами.
---
📋 Оглавление
Архитектура
Сервисы
Модели данных
Kafka события
Технологический стек
Установка и запуск
API Endpoints и примеры
Взаимодействие сервисов
---
🏗️ Архитектура
```
┌──────────────────────────────────────────────────────────────────┐
│                          API Gateway                             │
│                       (Port 8080)                                │
│  - JWT Validation & Token Processing                             │
│  - Reverse Proxy для микросервисов                               │
│  - Injection of X-User-ID и X-User-Role headers                  │
└────┬────────────┬──────────────┬──────────────┬──────────────────┘
     │            │              │              │
     ▼            ▼              ▼              ▼
┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────────┐
│   Auth   │ │ Booking  │ │ Catalog  │ │ Notifications &  │
│ Service  │ │ Service  │ │ Service  │ │    Audit Service │
│ :8081    │ │ :8082    │ │ :8083    │ │      :8084       │
└────┬─────┘ └────┬─────┘ └────┬─────┘ └────────┬─────────┘
     │            │            │                │
     └────────────┴────────────┴────────────────┘
              │
    ┌─────────▼──────────────┐
    │  Apache Kafka 3.7.0    │
    │   (Message Broker)     │
    │    Port 9092           │
    │                        │
    │ Topics:                │
    │ • users.events         │
    │ • bookings.events      │
    │ • catalog.events       │
    └────────┬───────────────┘
             │
    ┌────────┴──────────┐
    ▼                   ▼
┌─────────────┐   ┌──────────────────┐
│ PostgreSQL  │   │ PostgreSQL       │
│  :5433      │   │ :5434            │
│ (Auth DB)   │   │(Notifications DB)│
└─────────────┘   └──────────────────┘

Kafka UI: http://localhost:8090
```
---
🚀 Сервисы
1️⃣ API Gateway (Port 8080)
Назначение: Единая точка входа для всех клиентов, валидация JWT токенов, маршрутизация запросов.
Функции:
Валидация JWT токенов из Authorization header
Маршрутизация к микросервисам через reverse proxy
Внедрение headers: `X-User-ID`, `X-User-Role`
Разделение защищённых и открытых маршрутов
Конфигурация (.env):
```
USER_URL=http://auth-service:8081
CATALOG_URL=http://catalog-service:8083
BOOKING_URL=http://booking-service:8082
NOTIFICATIONS_AUDIT_URL=http://notifications-audit-service:8084
```
---
2️⃣ Auth Service (Port 8081)
Назначение: Аутентификация пользователей, управление учетными записями, выдача JWT токенов.
Функции:
Регистрация пользователей
Вход в систему (выдача JWT токена)
Получение профиля пользователя
Поддержка ролей: `client`, `admin`
Публикация событий в Kafka
Модель User:
```go
type User struct {
    ID           uint      // Уникальный идентификатор
    Name         string    // Имя (2-100 символов)
    Email        string    // Email (уникальный, валидный)
    PasswordHash string    // Хеш пароля (не передается в ответах)
    Role         Role      // "client" или "admin"
    CreatedAt    time.Time // Дата создания
    UpdatedAt    time.Time // Дата обновления
}
```
DTO для регистрации:
```go
type UserRegister struct {
    Name      string  // обязательно, 2-100 символов
    Email     string  // обязательно, валидный email, макс 255 символов
    Password  string  // обязательно, 8-255 символов
    AdminCode *string // опционально, для регистрации админа
}
```
DTO для входа:
```go
type UserLogin struct {
    Email    string // обязательно
    Password string // обязательно
}
```
---
3️⃣ Booking Service (Port 8082)
Назначение: Управление бронированиями/назначениями услуг.
Функции:
Создание, получение, удаление бронирований
Изменение статуса бронирования
Получение бронирований клиента или специалиста
Синхронизация данных с Catalog Service через Kafka
Публикация событий бронирований
Статусы бронирования:
```
- "created"   // Только что создано
- "confirmed" // Подтверждено
- "cancelled" // Отменено
- "completed" // Завершено
```
Модель Appointment:
```go
type Appointment struct {
    ID           uint
    ClientID     uint       // ID клиента
    SpecialistID uint       // ID специалиста
    ServiceID    uint       // ID услуги
    Weekday      string     // День недели
    StartTime    *time.Time // Время начала
    EndTime      *time.Time // Время окончания
    Status       Status     // Статус бронирования
    CreatedAt    time.Time
    UpdatedAt    time.Time
}
```
DTO для создания бронирования:
```go
type AppointmentCreateRequest struct {
    ClientID     uint      // Автоматически из X-User-ID header
    SpecialistID *uint     // Опционально, ID специалиста
    Weekday      *string   // Опционально, день недели
    ServiceID    *uint     // Опционально, ID услуги
    StartTime    *string   // Опционально
    EndTime      time.Time // Обязательно
    Status       Status    // Статус
}
```
Middleware аутентификации:
Требует заголовок `X-User-ID`
Требует заголовок `X-User-Role`
Разные endpoints для разных ролей
---
4️⃣ Catalog Service (Port 8083)
Назначение: Управление услугами, специалистами и их расписанием.
Функции:
Управление услугами (CRUD)
Управление специалистами (CRUD)
Управление расписанием специалистов
Связь услуг со специалистами
Потребление событий из Kafka
Публикация событий об изменениях
Модель Service:
```go
type Service struct {
    ID              uint   // Уникальный ID
    Title           string // Название (2-120 символов)
    Description     string // Описание (макс 2000 символов)
    DurationMinutes int    // Длительность в минутах (1-1440)
    Price           int    // Цена в копейках (>=0)
    IsActive        bool   // Активна ли услуга (по умолчанию true)
    CreatedAt       time.Time
    UpdatedAt       time.Time
}
```
Модель Specialist:
```go
type Specialist struct {
    ID          uint   // Уникальный ID
    Name        string // Имя (2-100 символов)
    Description string // Описание (макс 1000 символов)
    IsActive    bool   // Активен ли (по умолчанию true)
    CreatedAt   time.Time
    UpdatedAt   time.Time
}
```
Модель SpecialistSchedule:
```go
type SpecialistSchedule struct {
    ID           uint      // Уникальный ID
    SpecialistID uint      // ID специалиста
    Weekday      string    // День недели (monday-sunday)
    StartTime    time.Time // Время начала работы
    EndTime      time.Time // Время окончания работы
    CreatedAt    time.Time
    UpdatedAt    time.Time
}
```
Модель SpecialistService:
```go
type SpecialistService struct {
    ID           uint // Уникальный ID
    SpecialistID uint // ID специалиста
    ServiceID    uint // ID услуги
    CreatedAt    time.Time
    UpdatedAt    time.Time
}
```
DTO для услуги:
```go
// Создание
type CreateServiceRequest struct {
    Title           string `json:"title" binding:"required,min=2,max=120"`
    Description     string `json:"description" binding:"required,max=2000"`
    DurationMinutes int    `json:"duration_minutes" binding:"required,gt=0,lte=1440"`
    Price           int    `json:"price" binding:"required,gte=0"`
    IsActive        bool   `json:"is_active"`
}

// Обновление (все поля опциональны)
type UpdateServiceRequest struct {
    Title           *string `json:"title" binding:"omitempty,min=2,max=120"`
    Description     *string `json:"description" binding:"omitempty,max=2000"`
    DurationMinutes *int    `json:"duration_minutes" binding:"omitempty,gt=0,lte=1440"`
    Price           *int    `json:"price" binding:"omitempty,gte=0"`
    IsActive        *bool   `json:"is_active"`
}
```
DTO для специалиста:
```go
// Создание
type SpecialistCreateRequest struct {
    Name        string `json:"name" binding:"required,min=2,max=100"`
    Description string `json:"description" binding:"required,max=1000"`
    IsActive    bool   `json:"is_active"`
}

// Обновление (все поля опциональны)
type SpecialistUpdateRequest struct {
    Name        *string `json:"name" binding:"omitempty,min=2,max=100"`
    Description *string `json:"description" binding:"omitempty,max=1000"`
    IsActive    *bool   `json:"is_active"`
}
```
DTO для расписания:
```go
// Создание
type ScheduleCreateRequest struct {
    Weekday   string    `json:"weekday" binding:"required,oneof=monday tuesday wednesday thursday friday saturday sunday"`
    StartTime time.Time `json:"start_time" binding:"required"`
    EndTime   time.Time `json:"end_time" binding:"required"`
}

// Обновление (все поля опциональны)
type ScheduleUpdateRequest struct {
    Weekday   *string    `json:"weekday" binding:"omitempty,oneof=monday tuesday wednesday thursday friday saturday sunday"`
    StartTime *time.Time `json:"start_time" binding:"omitempty"`
    EndTime   *time.Time `json:"end_time" binding:"omitempty"`
}
```
DTO для связи специалист-услуга:
```go
type CreateSpecServ struct {
    ServiceID    uint `json:"service_id" binding:"required,gt=0"`
    SpecialistID uint `json:"specialist_id" binding:"required,gt=0"`
}
```
Middleware защиты:
Endpoints создания/обновления/удаления требуют роль `admin`
GET endpoints доступны всем
---
5️⃣ Notifications & Audit Service (Port 8084)
Назначение: Логирование всех событий и управление уведомлениями.
Функции:
Потребление событий из всех Kafka топиков
Создание записей аудита для каждого события
Создание и хранение уведомлений
Предоставление истории уведомлений пользователям
Типы уведомлений:
```
- "welcome"              // Приветственное уведомление при регистрации
- "booking_created"      // Когда создано новое бронирование
- "booking_cancelled"    // Когда бронирование отменено
- "booking_completed"    // Когда бронирование завершено
- "general"              // Общее уведомление
```
Модель Notification:
```go
type Notification struct {
    ID        uint             // Уникальный ID
    UserID    uint             // ID пользователя
    Type      NotificationType // Тип уведомления
    Title     string           // Заголовок
    Message   string           // Текст сообщения
    IsRead    bool             // Прочитано ли
    CreatedAt time.Time
    UpdatedAt time.Time
}
```
Модель AuditLog:
```go
type AuditLog struct {
    ID            uint      // Уникальный ID
    EventType     string    // Тип события (e.g., "user.registered")
    ActorID       uint      // ID пользователя, совершившего действие
    EntityType    string    // Тип сущности (e.g., "user", "appointment")
    EntityID      uint      // ID сущности
    SourceService string    // Сервис-источник события
    Payload       string    // JSON payload события
    CreatedAt     time.Time
    UpdatedAt     time.Time
}
```
---
📊 Модели данных
Иерархия сущностей
```
User (Auth Service)
├─ создается в Auth Service
└─ события публикуются в users.events
   └─ Notifications & Audit Service: создает записи аудита

Service (Catalog Service)
├─ управляется в Catalog Service
└─ события публикуются в catalog.events
   └─ Booking Service: синхронизирует данные

Specialist (Catalog Service)
├─ управляется в Catalog Service
├─ может быть привязан к Service через SpecialistService
├─ может иметь расписание через SpecialistSchedule
└─ события публикуются в catalog.events
   └─ Booking Service, Notifications & Audit: синхронизируют данные

Appointment (Booking Service)
├─ создается при бронировании
├─ связывает: Client (User), Service, Specialist
├─ события публикуются в bookings.events
└─ Catalog Service, Notifications & Audit: получают события
```
---
📨 Kafka события
1. Topic: `users.events`
Производитель: Auth Service  
Потребители: Notifications & Audit Service
События:
1.1 `user.registered` (при регистрации)
```json
{
  "event": "user.registered",
  "user_id": 1,
  "email": "user@example.com",
  "name": "John Doe",
  "role": "client",
  "timestamp": "2024-01-15T10:30:00Z"
}
```
Действие Notifications & Audit Service:
Создает AuditLog запись
Создает Notification типа "welcome" для пользователя
---
2. Topic: `catalog.events`
Производитель: Catalog Service  
Потребители: Booking Service, Notifications & Audit Service
События:
2.1 `specialist.created` (создание специалиста)
```json
{
  "event": "specialist.created",
  "specialist_id": 1,
  "name": "Dr. Smith",
  "description": "Professional specialist",
  "is_active": true,
  "timestamp": "2024-01-15T09:00:00Z"
}
```
2.2 `specialist.updated` (обновление специалиста)
```json
{
  "event": "specialist.updated",
  "specialist_id": 1,
  "name": "Dr. Smith",
  "is_active": true,
  "timestamp": "2024-01-15T09:15:00Z"
}
```
2.3 `specialist.deleted` (удаление специалиста)
```json
{
  "event": "specialist.deleted",
  "specialist_id": 1,
  "timestamp": "2024-01-15T09:30:00Z"
}
```
2.4 `specialist.service_attached` (привязка услуги к специалисту)
```json
{
  "event": "specialist.service_attached",
  "specialist_id": 1,
  "service_id": 5,
  "timestamp": "2024-01-15T09:45:00Z"
}
```
2.5 `specialist.schedule_updated` (обновление расписания)
```json
{
  "event": "specialist.schedule_updated",
  "specialist_id": 1,
  "weekday": "monday",
  "start_time": "09:00:00",
  "end_time": "17:00:00",
  "timestamp": "2024-01-15T10:00:00Z"
}
```
2.6 `specialist.service_deleted` (отвязка услуги от специалиста)
```json
{
  "event": "specialist.service_deleted",
  "specialist_id": 1,
  "service_id": 5,
  "timestamp": "2024-01-15T10:15:00Z"
}
```
2.7 `service.created` (создание услуги)
```json
{
  "event": "service.created",
  "service_id": 5,
  "title": "Consultation",
  "description": "Professional consultation",
  "duration_minutes": 30,
  "price": 5000,
  "is_active": true,
  "timestamp": "2024-01-15T08:00:00Z"
}
```
2.8 `service.updated` (обновление услуги)
```json
{
  "event": "service.updated",
  "service_id": 5,
  "title": "Consultation",
  "duration_minutes": 30,
  "price": 5000,
  "is_active": true,
  "timestamp": "2024-01-15T08:15:00Z"
}
```
2.9 `service.deleted` (удаление услуги)
```json
{
  "event": "service.deleted",
  "service_id": 5,
  "timestamp": "2024-01-15T08:30:00Z"
}
```
---
3. Topic: `bookings.events`
Производитель: Booking Service  
Потребители: Catalog Service, Notifications & Audit Service
События:
3.1 `appointment.created` (создание бронирования)
```json
{
  "event": "appointment.created",
  "appointment_id": 10,
  "client_id": 1,
  "specialist_id": 1,
  "service_id": 5,
  "weekday": "monday",
  "start_time": "2024-01-22T14:00:00Z",
  "end_time": "2024-01-22T14:30:00Z",
  "status": "created",
  "timestamp": "2024-01-15T10:35:00Z"
}
```
Действие Notifications & Audit Service:
Создает AuditLog запись
Создает Notification типа "booking_created" для клиента
3.2 `appointment.confirmed` (подтверждение бронирования)
```json
{
  "event": "appointment.confirmed",
  "appointment_id": 10,
  "status": "confirmed",
  "timestamp": "2024-01-15T10:40:00Z"
}
```
3.3 `appointment.cancelled` (отмена бронирования)
```json
{
  "event": "appointment.cancelled",
  "appointment_id": 10,
  "reason": "Client cancelled",
  "timestamp": "2024-01-15T10:45:00Z"
}
```
Действие Notifications & Audit Service:
Создает Notification типа "booking_cancelled"
3.4 `appointment.completed` (завершение бронирования)
```json
{
  "event": "appointment.completed",
  "appointment_id": 10,
  "timestamp": "2024-01-15T14:30:00Z"
}
```
---
🛠️ Технологический стек
Категория	Технология	Версия
Язык	Go	1.25.5 / 1.26.1
Framework	Gin	v1.12.0
ORM	GORM	v1.31.1
БД	PostgreSQL	17
Message Broker	Apache Kafka	3.7.0
Kafka Client	segmentio/kafka-go	v0.4.51
Аутентификация	golang-jwt	v5.3.1
Хеширование	golang.org/x/crypto	v0.53.0
Управление ENV	godotenv	v1.5.1
PostgreSQL Driver	github.com/jackc/pgx	v5
---
⚡ Установка и запуск
Предварительные требования
Docker & Docker Compose - для запуска контейнеров
Git - для клонирования репозитория
Запуск с Docker Compose
```bash
# 1. Клонируйте репозиторий
git clone https://github.com/Lastdabridge/Service-Booking-Microservices.git
cd Service-Booking-Microservices

# 2. Запустите все сервисы
docker-compose up --build

# 3. Проверьте статус сервисов
docker-compose ps
```
Доступные сервисы
Сервис	URL	Порт	Описание
API Gateway	`http://localhost:8080`	8080	Главный входной пункт
Auth Service	`http://localhost:8081`	8081	Аутентификация пользователей
Booking Service	`http://localhost:8082`	8082	Управление бронированиями
Catalog Service	`http://localhost:8083`	8083	Каталог услуг
Notifications & Audit	`http://localhost:8084`	8084	Уведомления и логирование
Kafka	`localhost:9092`	9092	Message Broker
Kafka UI	`http://localhost:8090`	8090	Web интерфейс для Kafka
Auth DB	`localhost:5433`	5433	PostgreSQL (Auth Service)
Notifications DB	`localhost:5434`	5434	PostgreSQL (Notifications & Audit)
Остановка сервисов
```bash
# Остановить все сервисы
docker-compose down

# Остановить и удалить данные
docker-compose down -v
```
---
🔌 API Endpoints и примеры
🔐 Auth Service (без аутентификации)
1. Регистрация пользователя
```
POST /api/auth/register
Content-Type: application/json
```
Валидный пример запроса:
```json
{
  "name": "John Doe",
  "email": "john@example.com",
  "password": "SecurePassword123"
}
```
Валидация:
`name`: обязательно, 2-100 символов
`email`: обязательно, валидный email, макс 255 символов
`password`: обязательно, 8-255 символов
Ответ (201 Created):
```json
{
  "message": "you have been successfully registered"
}
```
Ошибки:
400: `{"error": "validation error"}`
409: Email уже зарегистрирован
---
2. Вход в систему
```
POST /api/auth/login
Content-Type: application/json
```
Валидный пример запроса:
```json
{
  "email": "john@example.com",
  "password": "SecurePassword123"
}
```
Ответ (200 OK):
```json
{
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```
Ошибки:
400: `{"error": "validation error"}`
401: `{"error": "invalid credentials"}`
---
🔐 Auth Service (с аутентификацией)
3. Получить мой профиль
```
GET /api/auth/me
Authorization: Bearer <JWT_TOKEN>
```
Ответ (200 OK):
```json
{
  "user": {
    "id": 1,
    "name": "John Doe",
    "email": "john@example.com",
    "role": "client",
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z"
  }
}
```
Ошибки:
401: `{"error": "missing user context"}`
404: `{"error": "account not found"}`
---
📦 Catalog Service - Услуги
4. Получить все услуги (открыто)
```
GET /api/services/
```
Ответ (200 OK):
```json
[
  {
    "id": 1,
    "title": "Consultation",
    "description": "Professional consultation service",
    "duration_minutes": 30,
    "price": 5000,
    "is_active": true,
    "created_at": "2024-01-15T08:00:00Z",
    "updated_at": "2024-01-15T08:00:00Z"
  },
  {
    "id": 2,
    "title": "Full Checkup",
    "description": "Complete health checkup",
    "duration_minutes": 60,
    "price": 10000,
    "is_active": true,
    "created_at": "2024-01-15T08:15:00Z",
    "updated_at": "2024-01-15T08:15:00Z"
  }
]
```
---
5. Создать новую услугу (только админ)
```
POST /api/services/
Authorization: Bearer <ADMIN_TOKEN>
Content-Type: application/json
```
Валидный пример запроса:
```json
{
  "title": "Massage",
  "description": "Relaxing massage therapy",
  "duration_minutes": 45,
  "price": 3500,
  "is_active": true
}
```
Валидация:
`title`: 2-120 символов
`description`: макс 2000 символов
`duration_minutes`: 1-1440 минут
`price`: >= 0
Ответ (201 Created):
```json
{
  "id": 3,
  "title": "Massage",
  "description": "Relaxing massage therapy",
  "duration_minutes": 45,
  "price": 3500,
  "is_active": true,
  "created_at": "2024-01-15T11:00:00Z",
  "updated_at": "2024-01-15T11:00:00Z"
}
```
---
6. Обновить услугу (только админ)
```
PATCH /api/services/:id
Authorization: Bearer <ADMIN_TOKEN>
Content-Type: application/json
```
Пример запроса (все поля опциональны):
```json
{
  "price": 4000,
  "is_active": true
}
```
Ответ (200 OK):
```json
{
  "id": 3,
  "title": "Massage",
  "description": "Relaxing massage therapy",
  "duration_minutes": 45,
  "price": 4000,
  "is_active": true,
  "created_at": "2024-01-15T11:00:00Z",
  "updated_at": "2024-01-15T11:05:00Z"
}
```
---
7. Удалить услугу (только админ)
```
DELETE /api/services/:id
Authorization: Bearer <ADMIN_TOKEN>
```
Ответ (200 OK):
```json
{
  "message": "удаление прошло успешно"
}
```
---
👨‍⚕️ Catalog Service - Специалисты
8. Получить всех специалистов (открыто)
```
GET /api/specialists/
```
Ответ (200 OK):
```json
[
  {
    "id": 1,
    "name": "Dr. Smith",
    "description": "Senior doctor with 10 years experience",
    "is_active": true,
    "created_at": "2024-01-15T09:00:00Z",
    "updated_at": "2024-01-15T09:00:00Z"
  }
]
```
---
9. Создать специалиста (только админ)
```
POST /api/specialists/
Authorization: Bearer <ADMIN_TOKEN>
Content-Type: application/json
```
Валидный пример запроса:
```json
{
  "name": "Dr. Johnson",
  "description": "Experienced dentist with specialization in pediatric dentistry",
  "is_active": true
}
```
Валидация:
`name`: 2-100 символов
`description`: 1-1000 символов
`is_active`: boolean
Ответ (201 Created):
```json
{
  "id": 2,
  "name": "Dr. Johnson",
  "description": "Experienced dentist with specialization in pediatric dentistry",
  "is_active": true,
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-15T10:00:00Z"
}
```
---
10. Обновить специалиста (только админ)
```
PATCH /api/specialists/:id
Authorization: Bearer <ADMIN_TOKEN>
Content-Type: application/json
```
Пример запроса:
```json
{
  "is_active": false
}
```
Ответ (200 OK):
```json
{
  "id": 2,
  "name": "Dr. Johnson",
  "description": "Experienced dentist with specialization in pediatric dentistry",
  "is_active": false,
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-15T10:30:00Z"
}
```
---
11. Удалить специалиста (только админ)
```
DELETE /api/specialists/:id
Authorization: Bearer <ADMIN_TOKEN>
```
Ответ (200 OK):
```json
{
  "message": "специалист удален"
}
```
---
📅 Catalog Service - Расписание
12. Получить расписание специалиста (открыто)
```
GET /api/specialists/:id/schedule
```
Ответ (200 OK):
```json
[
  {
    "id": 1,
    "specialist_id": 1,
    "weekday": "monday",
    "start_time": "2024-01-15T09:00:00Z",
    "end_time": "2024-01-15T17:00:00Z",
    "created_at": "2024-01-15T10:00:00Z",
    "updated_at": "2024-01-15T10:00:00Z"
  }
]
```
---
13. Создать расписание для специалиста (только админ)
```
POST /api/specialists/:id/schedule
Authorization: Bearer <ADMIN_TOKEN>
Content-Type: application/json
```
Валидный пример запроса:
```json
{
  "weekday": "monday",
  "start_time": "2024-01-22T09:00:00Z",
  "end_time": "2024-01-22T17:00:00Z"
}
```
Валидация:
`weekday`: monday | tuesday | wednesday | thursday | friday | saturday | sunday
`start_time`: datetime (обязательно)
`end_time`: datetime (обязательно)
Ответ (201 Created):
```json
{
  "id": 2,
  "specialist_id": 1,
  "weekday": "monday",
  "start_time": "2024-01-22T09:00:00Z",
  "end_time": "2024-01-22T17:00:00Z",
  "created_at": "2024-01-15T10:30:00Z",
  "updated_at": "2024-01-15T10:30:00Z"
}
```
---
14. Обновить расписание (только админ)
```
PATCH /api/specialists/:id/schedule
Authorization: Bearer <ADMIN_TOKEN>
Content-Type: application/json
```
Пример запроса:
```json
{
  "end_time": "2024-01-22T18:00:00Z"
}
```
Ответ (200 OK):
```json
{
  "id": 2,
  "specialist_id": 1,
  "weekday": "monday",
  "start_time": "2024-01-22T09:00:00Z",
  "end_time": "2024-01-22T18:00:00Z",
  "created_at": "2024-01-15T10:30:00Z",
  "updated_at": "2024-01-15T10:35:00Z"
}
```
---
15. Удалить расписание (только админ)
```
DELETE /api/specialists/:id/schedule
Authorization: Bearer <ADMIN_TOKEN>
```
Ответ (200 OK):
```json
{
  "message": "расписание удалено"
}
```
---
16. Привязать услугу к специалисту (только админ)
```
POST /api/services/services-specialist
Authorization: Bearer <ADMIN_TOKEN>
Content-Type: application/json
```
Пример запроса:
```json
{
  "specialist_id": 1,
  "service_id": 1
}
```
Ответ (201 Created):
```json
{
  "id": 1,
  "specialist_id": 1,
  "service_id": 1,
  "created_at": "2024-01-15T11:00:00Z",
  "updated_at": "2024-01-15T11:00:00Z"
}
```
---
17. Удалить привязку услуги (только админ)
```
DELETE /api/services/services-specialist/:id
Authorization: Bearer <ADMIN_TOKEN>
```
Ответ (200 OK):
```json
{
  "message": "запись удалена"
}
```
---
📋 Booking Service - Бронирования
18. Получить мои бронирования (клиент)
```
GET /api/appointments/my
Authorization: Bearer <CLIENT_TOKEN>
X-User-ID: <USER_ID>
X-User-Role: client
```
Ответ (200 OK):
```json
[
  {
    "id": 1,
    "client_id": 1,
    "specialist_id": 1,
    "service_id": 1,
    "weekday": "monday",
    "start_time": "2024-01-22T14:00:00Z",
    "end_time": "2024-01-22T14:30:00Z",
    "status": "confirmed",
    "created_at": "2024-01-15T12:00:00Z",
    "updated_at": "2024-01-15T12:00:00Z"
  }
]
```
Ошибки:
400: X-User-ID header отсутствует или невалиден
403: Роль не "client"
---
19. Получить все бронирования (админ)
```
GET /api/appointments/all
Authorization: Bearer <ADMIN_TOKEN>
X-User-ID: <USER_ID>
X-User-Role: admin
```
Ответ (200 OK):
```json
[
  {
    "id": 1,
    "client_id": 1,
    "specialist_id": 1,
    "service_id": 1,
    "weekday": "monday",
    "start_time": "2024-01-22T14:00:00Z",
    "end_time": "2024-01-22T14:30:00Z",
    "status": "confirmed",
    "created_at": "2024-01-15T12:00:00Z",
    "updated_at": "2024-01-15T12:00:00Z"
  }
]
```
---
20. Получить бронирования специалиста (админ или специалист)
```
GET /api/appointments/specialist/:id
Authorization: Bearer <TOKEN>
X-User-ID: <USER_ID>
X-User-Role: admin | specialist
```
Ответ (200 OK):
```json
[
  {
    "id": 1,
    "client_id": 1,
    "specialist_id": 1,
    "service_id": 1,
    "weekday": "monday",
    "start_time": "2024-01-22T14:00:00Z",
    "end_time": "2024-01-22T14:30:00Z",
    "status": "confirmed",
    "created_at": "2024-01-15T12:00:00Z",
    "updated_at": "2024-01-15T12:00:00Z"
  }
]
```
---
21. Создать бронирование (клиент)
```
POST /api/appointments/
Authorization: Bearer <CLIENT_TOKEN>
X-User-ID: <USER_ID>
X-User-Role: client
Content-Type: application/json
```
Валидный пример запроса:
```json
{
  "specialist_id": 1,
  "service_id": 1,
  "weekday": "monday",
  "start_time": "2024-01-22T14:00:00Z",
  "end_time": "2024-01-22T14:30:00Z",
  "status": "created"
}
```
Примечание: `client_id` автоматически берется из заголовка `X-User-ID`
Ответ (201 Created):
```json
{
  "id": 10,
  "client_id": 1,
  "specialist_id": 1,
  "service_id": 1,
  "weekday": "monday",
  "start_time": "2024-01-22T14:00:00Z",
  "end_time": "2024-01-22T14:30:00Z",
  "status": "created",
  "created_at": "2024-01-15T12:30:00Z",
  "updated_at": "2024-01-15T12:30:00Z"
}
```
Ошибки:
400: Невалидные данные
404: Услуга, специалист или привязка не найдены
---
22. Изменить статус бронирования (админ)
```
PATCH /api/appointments/:id/status
Authorization: Bearer <ADMIN_TOKEN>
X-User-ID: <USER_ID>
X-User-Role: admin
Content-Type: application/json
```
Пример запроса:
```json
{
  "status": "confirmed"
}
```
Доступные статусы:
`created` → `confirmed`
`confirmed` → `completed` или `cancelled`
`cancelled` (финальный)
`completed` (финальный)
Ответ (200 OK):
```json
{
  "id": 10,
  "client_id": 1,
  "specialist_id": 1,
  "service_id": 1,
  "weekday": "monday",
  "start_time": "2024-01-22T14:00:00Z",
  "end_time": "2024-01-22T14:30:00Z",
  "status": "confirmed",
  "created_at": "2024-01-15T12:30:00Z",
  "updated_at": "2024-01-15T12:35:00Z"
}
```
---
23. Удалить бронирование (клиент может удалить свое, админ - любое)
```
DELETE /api/appointments/:id
Authorization: Bearer <TOKEN>
X-User-ID: <USER_ID>
X-User-Role: client | admin
```
Ответ (204 No Content)
Ошибки:
403: Клиент пытается удалить чужое бронирование
404: Бронирование не найдено
---
🔄 Взаимодействие сервисов
Сценарий 1: Полная регистрация и вход
```
┌─────────────────────────────────────────────────────────────┐
│ 1. Клиент отправляет POST /api/auth/register в Gateway      │
└──────────────┬──────────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────────┐
│ 2. Gateway маршрутизирует в Auth Service                    │
└──────────────┬──────────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────────┐
│ 3. Auth Service:                                            │
│    • Валидирует email и пароль                              │
│    • Хеширует пароль                                        │
│    • Сохраняет User в PostgreSQL (auth_db)                 │
│    • Публикует "user.registered" в users.events             │
└──────────────┬──────────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────────┐
│ 4. Notifications & Audit Service получает событие:          │
│    • Создает AuditLog запись                               │
│    • Создает Notification типа "welcome"                   │
└──────────────┬──────────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────────┐
│ 5. Клиент получает подтверждение регистрации                │
└─────────────────────────────────────────────────────────────┘
```
Сценарий 2: Создание бронирования
```
┌─────────────────────────────────────────────────────────────┐
│ 1. Клиент отправляет POST /api/appointments/ с JWT токеном  │
└──────────────┬──────────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────────┐
│ 2. Gateway валидирует JWT и маршрутизирует в Booking Service│
│    • Добавляет X-User-ID из токена                         │
│    • Добавляет X-User-Role из токена                       │
└──────────────┬──────────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────────┐
│ 3. Booking Service:                                         │
│    • Проверяет существование Service по ID                 │
│    • Проверяет существование Specialist по ID              │
│    • Проверяет привязку (SpecialistService)                │
│    • Создает Appointment в PostgreSQL                       │
│    • Публикует "appointment.created" в bookings.events      │
└──────────────┬──────────────────────────────────────────────┘
               │
┌──────────────┴──────────────────────────────────────────────┐
│                                                              │
▼                                                              ▼
Catalog Service получает событие:        Notifications & Audit Service:
• Логирует событие о новом                • Создает AuditLog запись
  бронировании                            • Создает Notification
• Обновляет информацию о                    типа "booking_created"
  расписании специалиста
```
Сценарий 3: Администратор создает новую услугу
```
┌─────────────────────────────────────────────────────────────┐
│ 1. Админ отправляет POST /api/services/ с админ JWT токеном │
└──────────────┬──────────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────────┐
│ 2. Gateway валидирует JWT (role == "admin")                 │
└──────────────┬──────────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────────┐
│ 3. Catalog Service:                                         │
│    • Проверяет роль админа через middleware                 │
│    • Валидирует данные услуги                               │
│    • Сохраняет Service в PostgreSQL                         │
│    • Публикует "service.created" в catalog.events           │
└──────────────┬──────────────────────────────────────────────┘
               │
┌──────────────┴──────────────────────────────────────────────┐
│                                                              │
▼                                                              ▼
Booking Service получает событие:        Notifications & Audit Service:
• Синхронизирует локальную БД с          • Создает AuditLog запись
  информацией о новой услуге             • Логирует создание услуги
```
---
🧪 Примеры использования с cURL
1. Регистрация
```bash
curl -X POST http://localhost:8080/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{
    "name": "John Doe",
    "email": "john@example.com",
    "password": "SecurePassword123"
  }'
```
2. Вход и получение токена
```bash
curl -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "john@example.com",
    "password": "SecurePassword123"
  }'

# Сохраните полученный token
export TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
```
3. Получить профиль
```bash
curl -X GET http://localhost:8080/api/auth/me \
  -H "Authorization: Bearer $TOKEN"
```
4. Получить услуги (без аутентификации)
```bash
curl -X GET http://localhost:8080/api/services/
```
5. Создать услугу (админ)
```bash
curl -X POST http://localhost:8080/api/services/ \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Consultation",
    "description": "Professional consultation service",
    "duration_minutes": 30,
    "price": 5000,
    "is_active": true
  }'
```
6. Создать специалиста (админ)
```bash
curl -X POST http://localhost:8080/api/specialists/ \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Dr. Smith",
    "description": "Senior doctor with 10 years experience",
    "is_active": true
  }'
```
7. Привязать услугу к специалисту (админ)
```bash
curl -X POST http://localhost:8080/api/services/services-specialist \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "specialist_id": 1,
    "service_id": 1
  }'
```
8. Создать расписание для специалиста (админ)
```bash
curl -X POST http://localhost:8080/api/specialists/1/schedule \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "weekday": "monday",
    "start_time": "2024-01-22T09:00:00Z",
    "end_time": "2024-01-22T17:00:00Z"
  }'
```
9. Создать бронирование (клиент)
```bash
curl -X POST http://localhost:8080/api/appointments/ \
  -H "Authorization: Bearer $CLIENT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "specialist_id": 1,
    "service_id": 1,
    "weekday": "monday",
    "start_time": "2024-01-22T14:00:00Z",
    "end_time": "2024-01-22T14:30:00Z",
    "status": "created"
  }'
```
10. Получить мои бронирования (клиент)
```bash
curl -X GET http://localhost:8080/api/appointments/my \
  -H "Authorization: Bearer $CLIENT_TOKEN"
```
11. Изменить статус бронирования (админ)
```bash
curl -X PATCH http://localhost:8080/api/appointments/1/status \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "status": "confirmed"
  }'
```
---
🔍 Мониторинг и отладка
Просмотр логов
```bash
# Логи конкретного сервиса
docker-compose logs -f auth-service
docker-compose logs -f booking-service
docker-compose logs -f catalog-service
docker-compose logs -f gateway

# Логи всех сервисов
docker-compose logs -f
```
Kafka UI
Откройте в браузере: `http://localhost:8090`
Здесь вы можете:
📊 Просматривать все топики (`users.events`, `bookings.events`, `catalog.events`)
📨 Просматривать сообщения в каждом топике
📈 Отслеживать consumer groups
📉 Анализировать производительность
Подключение к PostgreSQL
```bash
# Auth Service DB
psql -h localhost -p 5433 -U postgres -d auth_db
Password: 5647

# Notifications & Audit Service DB
psql -h localhost -p 5434 -U postgres -d notifications_audit_service
Password: 4545

# Примеры запросов
\dt                    # Список таблиц
SELECT * FROM users;   # Все пользователи
SELECT * FROM audit_logs LIMIT 10; # Последние 10 событий аудита
```
Проверка статуса сервисов
```bash
docker-compose ps

# Вывод:
# NAME                                COMMAND                  SERVICE                           STATUS
# auth-service                        "go run cmd/app/main"    auth-service                      Up
# booking-service                     "go run cmd/main.go"     booking-service                   Up
# catalog-service                     "go run cmd/main.go"     catalog-service                   Up
# gateway                             "go run cmd/main.go"     gateway                           Up
# kafka                               "/etc/confluent/dock"    kafka                             Up (healthy)
# kafka-ui                            "/bin/sh -c 'java -cp"   kafka-ui                          Up
```
---
✅ Валидация и ошибки
Распространённые ошибки валидации
Ошибка	Причина	Решение
`400: error: validation error`	Невалидные данные в теле запроса	Проверьте формат JSON и типы полей
`401: error: invalid token`	JWT токен истёк или невалиден	Авторизуйтесь заново через login
`401: error: missing authorization header`	Отсутствует заголовок Authorization	Добавьте `Authorization: Bearer <token>`
`403: error: вы не admin`	Недостаточные права доступа	Используйте админ токен
`404: error: account not found`	Пользователь не найден	Проверьте X-User-ID
`409: email already exists`	Email уже зарегистрирован	Используйте другой email
---
📝 Примечания
Eventual Consistency: Данные в разных сервисах синхронизируются асинхронно через Kafka
Database per Service: Каждый микросервис имеет собственную БД
Event Sourcing: История всех событий сохраняется в Kafka и AuditLog таблице
JWT Token: Имеет срок действия, обновляется через повторный вход
Role-Based Access: Endpoints защищены проверкой роли пользователя
---
📞 Полезные команды
```bash
# Пересборка образов
docker-compose build

# Очистка всех данных
docker-compose down -v

# Просмотр логов в реальном времени
docker-compose logs -f

# Перезагрузка сервиса
docker-compose restart auth-service

# Вход в контейнер для отладки
docker-compose exec auth-service bash
```
---
Версия документации: 1.0  
Последнее обновление: Июль 2026  
