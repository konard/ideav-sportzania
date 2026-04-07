<?php

/**
 * Скрипт выборки данных из Яндекс Метрики.
 *
 * Получает основные показатели для дэшборда с агрегацией по дням
 * и записывает их в CSV-файлы с названиями столбцов на русском языке.
 *
 * Использование:
 *   php ym-fetch.php
 *
 * Конфигурация задаётся в файле ym-config.php.
 */

require_once __DIR__ . '/ym-config.php';

// -----------------------------------------------------------------------
// Вспомогательные функции
// -----------------------------------------------------------------------

/**
 * Выполняет GET-запрос к API Яндекс Метрики и возвращает декодированный JSON.
 *
 * @param string $url    URL метода API
 * @param array  $params Параметры запроса
 * @return array
 * @throws RuntimeException При HTTP-ошибке или невалидном ответе
 */
function ym_api_get(string $url, array $params): array
{
    $params['pretty'] = 0;
    $queryString = http_build_query($params);
    $fullUrl = $url . '?' . $queryString;

    $context = stream_context_create([
        'http' => [
            'method'  => 'GET',
            'header'  => "Authorization: OAuth " . YM_TOKEN . "\r\n" .
                         "Content-Type: application/x-yametrika+json\r\n",
            'timeout' => YM_HTTP_TIMEOUT,
        ],
    ]);

    $response = @file_get_contents($fullUrl, false, $context);

    if ($response === false) {
        $error = error_get_last();
        throw new RuntimeException("Ошибка HTTP-запроса к API: " . ($error['message'] ?? 'неизвестная ошибка'));
    }

    $data = json_decode($response, true);

    if (!is_array($data)) {
        throw new RuntimeException("Некорректный JSON-ответ от API: " . substr($response, 0, 200));
    }

    if (isset($data['errors'])) {
        $msg = json_encode($data['errors'], JSON_UNESCAPED_UNICODE);
        throw new RuntimeException("Ошибка API Яндекс Метрики: " . $msg);
    }

    return $data;
}

/**
 * Запрашивает отчёт Logs API / Stat API с разбивкой по дням (group=day).
 *
 * @param array  $metrics   Список метрик (ym:s:xxx)
 * @param array  $dimensions Список измерений (ym:s:xxx) — может быть пустым
 * @param string $dateFrom  Начало периода YYYY-MM-DD
 * @param string $dateTo    Конец периода YYYY-MM-DD
 * @param int    $offset    Смещение для пагинации
 * @return array  Ответ API (поле 'data' — строки, 'total_rows' — всего строк)
 */
function ym_stat_get(array $metrics, array $dimensions, string $dateFrom, string $dateTo, int $offset = 1): array
{
    $params = [
        'id'         => YM_COUNTER_ID,
        'metrics'    => implode(',', $metrics),
        'date1'      => $dateFrom,
        'date2'      => $dateTo,
        'group'      => 'day',
        'limit'      => YM_API_LIMIT,
        'offset'     => $offset,
        'accuracy'   => 'full',
        'lang'       => 'ru',
    ];

    if (!empty($dimensions)) {
        $params['dimensions'] = implode(',', $dimensions);
    }

    return ym_api_get('https://api-metrika.yandex.net/stat/v1/data', $params);
}

/**
 * Сохраняет строки данных в CSV-файл.
 *
 * @param string   $filename  Имя файла (без пути, без расширения)
 * @param array    $headers   Заголовки столбцов на русском языке
 * @param array    $rows      Двумерный массив строк
 * @return string  Путь к созданному файлу
 */
function ym_write_csv(string $filename, array $headers, array $rows): string
{
    $dir = YM_OUTPUT_DIR;
    if (!is_dir($dir) && !mkdir($dir, 0755, true)) {
        throw new RuntimeException("Не удалось создать директорию: $dir");
    }

    $path = $dir . DIRECTORY_SEPARATOR . $filename . '.csv';
    $fp = fopen($path, 'w');
    if ($fp === false) {
        throw new RuntimeException("Не удалось открыть файл для записи: $path");
    }

    // BOM для корректного отображения кирилицы в Excel
    fwrite($fp, "\xEF\xBB\xBF");

    fputcsv($fp, $headers, ';');
    foreach ($rows as $row) {
        fputcsv($fp, $row, ';');
    }

    fclose($fp);
    return $path;
}

/**
 * Извлекает все строки из постраничного ответа Stat API.
 *
 * @param array  $metrics    Список метрик
 * @param array  $dimensions Список измерений
 * @return array  Все строки в формате [['dimensions' => [...], 'metrics' => [...]], ...]
 */
function ym_fetch_all_rows(array $metrics, array $dimensions): array
{
    $allRows  = [];
    $offset   = 1;

    do {
        $response   = ym_stat_get($metrics, $dimensions, YM_DATE_FROM, YM_DATE_TO, $offset);
        $data       = $response['data']       ?? [];
        $totalRows  = $response['total_rows'] ?? 0;

        $allRows = array_merge($allRows, $data);
        $offset += count($data);
    } while ($offset <= $totalRows && count($data) > 0);

    return $allRows;
}

/**
 * Форматирует значение метрики для CSV (округление float, замена точки на запятую).
 *
 * @param mixed $value
 * @return string
 */
function ym_format_value($value): string
{
    if (is_float($value)) {
        return number_format($value, 4, ',', '');
    }
    return (string) $value;
}

/**
 * Возвращает значение измерения из строки ответа API.
 *
 * @param array  $dimValues  Массив объектов измерений из строки
 * @param int    $index      Индекс измерения
 * @return string
 */
function ym_dim_value(array $dimValues, int $index): string
{
    $val = $dimValues[$index] ?? [];
    // Stat API возвращает либо 'name', либо 'id'
    return $val['name'] ?? $val['id'] ?? '';
}

// -----------------------------------------------------------------------
// Функции для каждого раздела дэшборда
// -----------------------------------------------------------------------

/**
 * 1. Посещаемость и вовлеченность — агрегация по дням.
 * Dimensions: дата (ym:s:date)
 * Metrics: визиты, посетители, просмотры, новые посетители,
 *          глубина, время, отказы.
 */
function fetch_traffic_engagement(): void
{
    echo "  Запрашиваем: Посещаемость и вовлеченность...\n";

    $dimensions = ['ym:s:date'];
    $metrics = [
        'ym:s:visits',
        'ym:s:users',
        'ym:s:pageviews',
        'ym:s:newUsers',
        'ym:s:pageDepth',
        'ym:s:visitDuration',
        'ym:s:bounceRate',
    ];

    $rows = ym_fetch_all_rows($metrics, $dimensions);

    $headers = [
        'Дата',
        'Визиты',
        'Посетители',
        'Просмотры страниц',
        'Новые посетители',
        'Средняя глубина просмотра',
        'Среднее время на сайте (сек)',
        'Отказы (%)',
    ];

    $csvRows = [];
    foreach ($rows as $row) {
        $csvRows[] = [
            ym_dim_value($row['dimensions'], 0),
            ym_format_value($row['metrics'][0] ?? ''),
            ym_format_value($row['metrics'][1] ?? ''),
            ym_format_value($row['metrics'][2] ?? ''),
            ym_format_value($row['metrics'][3] ?? ''),
            ym_format_value($row['metrics'][4] ?? ''),
            ym_format_value($row['metrics'][5] ?? ''),
            ym_format_value($row['metrics'][6] ?? ''),
        ];
    }

    $path = ym_write_csv('01_посещаемость_и_вовлеченность', $headers, $csvRows);
    echo "  Сохранено: $path (" . count($csvRows) . " строк)\n";
}

/**
 * 2. Конверсии и эффективность по целям.
 * Dimensions: дата, цель
 * Метрики запрашиваются для каждой цели из YM_GOAL_IDS.
 */
function fetch_conversions(): void
{
    $goalIds = YM_GOAL_IDS;

    if (empty($goalIds)) {
        echo "  Пропускаем: Конверсии (YM_GOAL_IDS не заданы в конфиге).\n";
        return;
    }

    echo "  Запрашиваем: Конверсии и эффективность...\n";

    $dimensions = ['ym:s:date'];
    $metrics = [];
    foreach ($goalIds as $goalId) {
        $metrics[] = "ym:s:goal{$goalId}conversionRate";
        $metrics[] = "ym:s:goal{$goalId}reaches";
    }

    $rows = ym_fetch_all_rows($metrics, $dimensions);

    $headers = ['Дата'];
    foreach ($goalIds as $goalId) {
        $headers[] = "Конверсия цели $goalId (%)";
        $headers[] = "Достижения цели $goalId";
    }

    $csvRows = [];
    foreach ($rows as $row) {
        $line = [ym_dim_value($row['dimensions'], 0)];
        foreach ($row['metrics'] as $metricValue) {
            $line[] = ym_format_value($metricValue);
        }
        $csvRows[] = $line;
    }

    $path = ym_write_csv('02_конверсии_и_эффективность', $headers, $csvRows);
    echo "  Сохранено: $path (" . count($csvRows) . " строк)\n";
}

/**
 * 3. Источники трафика — распределение визитов по каналам.
 * Dimensions: дата, источник трафика
 * Metrics: визиты + конверсия (если заданы цели).
 */
function fetch_traffic_sources(): void
{
    echo "  Запрашиваем: Источники трафика...\n";

    $goalIds    = YM_GOAL_IDS;
    $dimensions = ['ym:s:date', 'ym:s:trafficSource'];
    $metrics    = ['ym:s:visits'];

    foreach ($goalIds as $goalId) {
        $metrics[] = "ym:s:goal{$goalId}conversionRate";
    }

    $rows = ym_fetch_all_rows($metrics, $dimensions);

    $headers = ['Дата', 'Источник трафика', 'Визиты'];
    foreach ($goalIds as $goalId) {
        $headers[] = "Конверсия цели $goalId (%)";
    }

    $csvRows = [];
    foreach ($rows as $row) {
        $line = [
            ym_dim_value($row['dimensions'], 0),
            ym_dim_value($row['dimensions'], 1),
            ym_format_value($row['metrics'][0] ?? ''),
        ];
        for ($i = 1; $i < count($row['metrics']); $i++) {
            $line[] = ym_format_value($row['metrics'][$i]);
        }
        $csvRows[] = $line;
    }

    $path = ym_write_csv('03_источники_трафика', $headers, $csvRows);
    echo "  Сохранено: $path (" . count($csvRows) . " строк)\n";
}

/**
 * 4а. UTM-метки — детализация по utm_source / utm_medium / utm_campaign / utm_content / utm_term.
 * Dimensions: дата, utm-параметры
 * Metrics: визиты + конверсии по целям.
 */
function fetch_utm(): void
{
    echo "  Запрашиваем: UTM-метки...\n";

    $goalIds    = YM_GOAL_IDS;
    $dimensions = [
        'ym:s:date',
        'ym:s:UTMSource',
        'ym:s:UTMMedium',
        'ym:s:UTMCampaign',
        'ym:s:UTMContent',
        'ym:s:UTMTerm',
    ];
    $metrics = ['ym:s:visits'];
    foreach ($goalIds as $goalId) {
        $metrics[] = "ym:s:goal{$goalId}conversionRate";
        $metrics[] = "ym:s:goal{$goalId}reaches";
    }

    $rows = ym_fetch_all_rows($metrics, $dimensions);

    $headers = [
        'Дата',
        'UTM Source',
        'UTM Medium',
        'UTM Campaign',
        'UTM Content',
        'UTM Term',
        'Визиты',
    ];
    foreach ($goalIds as $goalId) {
        $headers[] = "Конверсия цели $goalId (%)";
        $headers[] = "Достижения цели $goalId";
    }

    $csvRows = [];
    foreach ($rows as $row) {
        $line = [
            ym_dim_value($row['dimensions'], 0),
            ym_dim_value($row['dimensions'], 1),
            ym_dim_value($row['dimensions'], 2),
            ym_dim_value($row['dimensions'], 3),
            ym_dim_value($row['dimensions'], 4),
            ym_dim_value($row['dimensions'], 5),
            ym_format_value($row['metrics'][0] ?? ''),
        ];
        for ($i = 1; $i < count($row['metrics']); $i++) {
            $line[] = ym_format_value($row['metrics'][$i]);
        }
        $csvRows[] = $line;
    }

    $path = ym_write_csv('04_utm_метки', $headers, $csvRows);
    echo "  Сохранено: $path (" . count($csvRows) . " строк)\n";
}

/**
 * 5. Аудитория — география.
 * Dimensions: дата, страна, город
 * Metrics: визиты.
 */
function fetch_audience_geo(): void
{
    echo "  Запрашиваем: Аудитория — география...\n";

    $dimensions = ['ym:s:date', 'ym:s:regionCountry', 'ym:s:regionCity'];
    $metrics    = ['ym:s:visits'];

    $rows = ym_fetch_all_rows($metrics, $dimensions);

    $headers = ['Дата', 'Страна', 'Город', 'Визиты'];

    $csvRows = [];
    foreach ($rows as $row) {
        $csvRows[] = [
            ym_dim_value($row['dimensions'], 0),
            ym_dim_value($row['dimensions'], 1),
            ym_dim_value($row['dimensions'], 2),
            ym_format_value($row['metrics'][0] ?? ''),
        ];
    }

    $path = ym_write_csv('05_аудитория_география', $headers, $csvRows);
    echo "  Сохранено: $path (" . count($csvRows) . " строк)\n";
}

/**
 * 6. Аудитория — устройства.
 * Dimensions: дата, тип устройства
 * Metrics: визиты.
 */
function fetch_audience_devices(): void
{
    echo "  Запрашиваем: Аудитория — устройства...\n";

    $dimensions = ['ym:s:date', 'ym:s:deviceCategory'];
    $metrics    = ['ym:s:visits'];

    $rows = ym_fetch_all_rows($metrics, $dimensions);

    $headers = ['Дата', 'Тип устройства', 'Визиты'];

    $csvRows = [];
    foreach ($rows as $row) {
        $csvRows[] = [
            ym_dim_value($row['dimensions'], 0),
            ym_dim_value($row['dimensions'], 1),
            ym_format_value($row['metrics'][0] ?? ''),
        ];
    }

    $path = ym_write_csv('06_аудитория_устройства', $headers, $csvRows);
    echo "  Сохранено: $path (" . count($csvRows) . " строк)\n";
}

/**
 * 7. Аудитория — пол и возраст.
 * Dimensions: дата, пол, возраст
 * Metrics: визиты.
 */
function fetch_audience_demographics(): void
{
    echo "  Запрашиваем: Аудитория — пол и возраст...\n";

    $dimensions = ['ym:s:date', 'ym:s:gender', 'ym:s:age'];
    $metrics    = ['ym:s:visits'];

    $rows = ym_fetch_all_rows($metrics, $dimensions);

    $headers = ['Дата', 'Пол', 'Возраст', 'Визиты'];

    $csvRows = [];
    foreach ($rows as $row) {
        $csvRows[] = [
            ym_dim_value($row['dimensions'], 0),
            ym_dim_value($row['dimensions'], 1),
            ym_dim_value($row['dimensions'], 2),
            ym_format_value($row['metrics'][0] ?? ''),
        ];
    }

    $path = ym_write_csv('07_аудитория_пол_и_возраст', $headers, $csvRows);
    echo "  Сохранено: $path (" . count($csvRows) . " строк)\n";
}

/**
 * 8. Технические метрики — время загрузки страниц.
 * Dimensions: дата
 * Metrics: pageLoadTime.
 */
function fetch_technical(): void
{
    echo "  Запрашиваем: Технические метрики...\n";

    $dimensions = ['ym:s:date'];
    $metrics    = ['ym:s:pageLoadTime'];

    $rows = ym_fetch_all_rows($metrics, $dimensions);

    $headers = ['Дата', 'Среднее время загрузки страниц (сек)'];

    $csvRows = [];
    foreach ($rows as $row) {
        $csvRows[] = [
            ym_dim_value($row['dimensions'], 0),
            ym_format_value($row['metrics'][0] ?? ''),
        ];
    }

    $path = ym_write_csv('08_технические_метрики', $headers, $csvRows);
    echo "  Сохранено: $path (" . count($csvRows) . " строк)\n";
}

// -----------------------------------------------------------------------
// Точка входа
// -----------------------------------------------------------------------

echo "=======================================================\n";
echo "Яндекс Метрика — выборка данных для дэшборда\n";
echo "=======================================================\n";
echo "Счётчик:  " . YM_COUNTER_ID . "\n";
echo "Период:   " . YM_DATE_FROM . " — " . YM_DATE_TO . "\n";
echo "Папка:    " . YM_OUTPUT_DIR . "\n";
echo "-------------------------------------------------------\n";

$sections = [
    'fetch_traffic_engagement',
    'fetch_conversions',
    'fetch_traffic_sources',
    'fetch_utm',
    'fetch_audience_geo',
    'fetch_audience_devices',
    'fetch_audience_demographics',
    'fetch_technical',
];

$errors = [];

foreach ($sections as $fn) {
    try {
        $fn();
    } catch (Throwable $e) {
        $msg = "  ОШИБКА в $fn(): " . $e->getMessage();
        echo $msg . "\n";
        $errors[] = $msg;
    }
}

echo "-------------------------------------------------------\n";

if (empty($errors)) {
    echo "Готово. Все данные успешно сохранены в: " . YM_OUTPUT_DIR . "\n";
} else {
    echo "Завершено с ошибками (" . count($errors) . "):\n";
    foreach ($errors as $err) {
        echo $err . "\n";
    }
    exit(1);
}
