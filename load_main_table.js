const mysql = require('mysql2/promise');

async function isDataExists(connection) {
    try {
        const [rows] = await connection.query(`
      SELECT COUNT(*) AS rowCount
      FROM control.log
      WHERE process_id = 3 AND status = 'successful' AND DATE_FORMAT(time, '%Y-%m-%d %H') = DATE_FORMAT(NOW(), '%Y-%m-%d %H');
    `);

        const rowCount = rows[0].rowCount;
        return rowCount > 0;
    } catch (error) {
        throw error;
    }
}

async function isRunning(connection) {
    try {
        const [rows] = await connection.query(`
      SELECT COUNT(*) AS rowCount
      FROM control.log
      WHERE process_id = 4 AND status = 'start' AND DATE_FORMAT(time, '%Y-%m-%d %H') = DATE_FORMAT(NOW(), '%Y-%m-%d %H');
    `);

        const rowCount = rows[0].rowCount;
        return rowCount == 0;
    } catch (error) {
        throw error;
    }
}

async function isSuccessful(connection) {
    try {
        const [rows] = await connection.query(`
            SELECT COUNT(*) AS rowSuccessful
            FROM control.log
            WHERE process_id = 4 AND status = 'successful' AND DATE_FORMAT(time, '%Y-%m-%d %H') = DATE_FORMAT(NOW(), '%Y-%m-%d %H');
        `);

        const rowSuccessful = rows[0].rowSuccessful;
        return rowSuccessful == 0;
    } catch (error) {
        throw error;
    }
}



async function connectToStaging() {
    try {
        const connectionStaging = await mysql.createConnection({
            host: "localhost",
            user: "root",
            password: "",
            database: "staging"
        });

        console.log('Đã kết nối đến cơ sở dữ liệu Staging');
        return connectionStaging;
    } catch (error) {
        console.error('Lỗi kết nối đến cơ sở dữ liệu Staging:', error);
        throw error; // Chuyển tiếp lỗi để báo hiệu rằng kết nối đã thất bại
    }
}

async function connectToDataWarehouse() {
    try {
        const connectionDatawarehouse = await mysql.createConnection({
            host: "localhost",
            user: "root",
            password: "",
            database: "datawarehouse"
        });

        console.log('Đã kết nối đến cơ sở dữ liệu DataWarehouse');
        return connectionDatawarehouse;
    } catch (error) {
        console.error('Lỗi kết nối đến cơ sở dữ liệu Datawarehouse:', error);
        throw error; // Chuyển tiếp lỗi để báo hiệu rằng kết nối đã thất bại
    }
}

async function main() {
    try {
        // 1. Connect to DB control
        const connectionControl = await mysql.createConnection({
            host: "localhost",
            user: "root",
            password: "",
            database: "control"
        });
        // Gọi hàm kiểm tra dữ liệu
        try {
            // Gọi hàm kiểm tra và xử lý kết quả
            const dataExists = await isDataExists(connectionControl);
            const dataRunning = await isRunning(connectionControl);
            const dataSuccessful = await isSuccessful(connectionControl);
            // 2.Check exist in log table has log: process_id: 3, status: successful, in the same hour
            if (dataExists) {
                // 4.Check not exist in log table has log: process_id: 4, status: start in the same hour
                if (dataRunning) {
                    // 6.Check not exist in log table has log: process_id: 4, status: successful in the same hour
                    if (dataSuccessful) {
                        // 8. Insert Table log(control):time:now, process:4,status:start
                        const queryLog =
                            "INSERT INTO control.log (time, process_id, status) VALUES (NOW(), ?, ?)";
                        const process_id = "4";
                        const status = "start";
                        const valuesLog = [process_id, status];
                        connectionControl.query(queryLog, valuesLog, (error, results) => {
                            if (error) throw error;
                            console.log("Insert log process: 4");
                        });
                        try {
                            // 9. Connect to DB staging
                            const connectionStaging = await connectToStaging();

                            console.log("Đã kết nối DB Staging");
                            // 12. Call to run query Create temporary table temp_data_province, temp_data_weather, temp_data_main_table
                            // to save data from second-table

                            // Tạo bảng nhớ tạm temp_data_province
                            const createTemporaryProvince = await connectionStaging.query(`
                                CREATE TEMPORARY TABLE IF NOT EXISTS temp_data_province (
                                    ID INT AUTO_INCREMENT PRIMARY KEY,
                                    province TEXT
                                )
                            `);

                            // Tạo bảng nhớ tạm temp_data_weather
                            const createTemporaryWeather  = await connectionStaging.query(`
                                CREATE TEMPORARY TABLE IF NOT EXISTS temp_data_weather (
                                    ID INT AUTO_INCREMENT PRIMARY KEY,
                                    weather TEXT
                                )
                            `);

                            // Tạo bảng nhớ tạm temp_data_main_table
                            const createTemporaryMainTable  = await connectionStaging.query(`
                                CREATE TEMPORARY TABLE IF NOT EXISTS temp_data_main_table (
                                    ID INT AUTO_INCREMENT PRIMARY KEY,
                                    time DATETIME,
                                    province_id INT,
                                    weather_id INT,
                                    temperature INT,
                                    humidity INT,
                                    t1 INT,
                                    t2 INT,
                                    t3 INT,
                                    t4 INT,
                                    t5 INT
                                )
                            `);
                            // 13. Check result of query Create temporary table temp_data_province, temp_data_weather, temp_data_main_table are true
                            if (createTemporaryProvince && createTemporaryWeather && createTemporaryMainTable){
                                console.log("Tạo bảng tạm thành công");
                                // 16. Call to run query Change data type column, caculate and insert data from second-table to temporary tables
                                const insertToTemporaryProvince  = await connectionStaging.query(`
                                    INSERT INTO temp_data_province (province)
                                                SELECT DISTINCT province FROM staging.second_table;
                                `);
                                const insertToTemporaryWeather  = await connectionStaging.query(`
                                    INSERT INTO temp_data_weather (weather)
                                                SELECT DISTINCT weather FROM staging.second_table;
                                `);
                                const insertToTemporaryMainTable  = await connectionStaging.query(`
                                    INSERT INTO temp_data_main_table ( time, province_id, weather_id, temperature, humidity, t1, t2, t3, t4, t5 )
                                                SELECT
                                                    STR_TO_DATE(st.time, '%H:%i:%s %d:%m:%Y'),
                                                    tp.ID AS province_id,
                                                    tw.ID AS weather_id,
                                                    AVG(CAST(temperature AS DECIMAL(10, 2))) AS temperature,
                                                    AVG(CAST(humidity AS DECIMAL(10, 2))) AS humidity,
                                                    AVG(CAST(t1 AS DECIMAL(10, 2))) AS t1,
                                                    AVG(CAST(t2 AS DECIMAL(10, 2))) AS t2,
                                                    AVG(CAST(t3 AS DECIMAL(10, 2))) AS t3,
                                                    AVG(CAST(t4 AS DECIMAL(10, 2))) AS t4,
                                                    AVG(CAST(t5 AS DECIMAL(10, 2))) AS t5
                                                FROM
                                                    staging.second_table st
                                                    JOIN temp_data_province tp ON st.province = tp.province
                                                    JOIN temp_data_weather tw ON st.weather = tw.weather
                                                GROUP BY
                                                    st.time,
                                                    st.province;
                                `);
                                // 17. Check result of query Change data type column, caculate and insert data from second-table to temporary tables are true
                                if (insertToTemporaryProvince && insertToTemporaryWeather && insertToTemporaryMainTable){
                                    console.log("Chuyển sang bộ nhớ ảo thành công");
                                    try {
                                        // 20. Connect to DB datawarehouse
                                        const connectionDatawarehouse = await connectToDataWarehouse();
                                        // 23. Call to run query Insert temp_data_province, temp_data_weather, temp_data_main_table into
                                        // to table dim_province, table dim_weather, table fact_main_table
                                        const insertToDim_Province  = await connectionStaging.query(`
                                            INSERT INTO datawarehouse.dim_province ( province, es_date, ee_date)
                                                        SELECT
                                                           
                                                            tp.province,
                                                            NOW() AS es_date,
                                                            STR_TO_DATE('31-12-9999 23:59:59', '%d-%m-%Y %H:%i:%s') AS ee_date
                                                        FROM temp_data_province tp
                                                        LEFT JOIN datawarehouse.dim_province dp ON tp.province = dp.province
                                                        WHERE dp.ID IS NULL
                                                        ORDER BY tp.ID;
                                        `);
                                        const insertToDim_Weather  = await connectionStaging.query(`
                                            INSERT INTO datawarehouse.dim_weather ( weather, es_date, ee_date)
                                                        SELECT
                                                           
                                                            tw.weather,
                                                            NOW() AS es_date,
                                                            STR_TO_DATE('31-12-9999 23:59:59', '%d-%m-%Y %H:%i:%s') AS ee_date
                                                        FROM temp_data_weather tw
                                                        LEFT JOIN datawarehouse.dim_weather dw ON tw.weather = dw.weather
                                                        WHERE dw.ID IS NULL
                                                        ORDER BY tw.ID;
                                        `);
                                        const insertToFact_Main_Table  = await connectionStaging.query(`
                                             INSERT INTO datawarehouse.fact_main_table (time, province_id, weather_id, temperature, humidity, t1, t2, t3, t4, t5 )
                                                        SELECT
                                                           
                                                            t1.time,
                                                            COALESCE( dp.ID, ( SELECT ID FROM datawarehouse.dim_province WHERE province = t2.province ) ) as province_id,
                                                            COALESCE( dw.ID, ( SELECT ID FROM datawarehouse.dim_weather WHERE weather = t3.weather ) ) as weather_id,
                                                            t1.temperature,
                                                            t1.humidity,
                                                            t1.t1,
                                                            t1.t2,
                                                            t1.t3,
                                                            t1.t4,
                                                            t1.t5
                                                        FROM
                                                            temp_data_main_table t1
                                                            JOIN temp_data_province t2 ON t1.province_id = t2.ID
                                                            JOIN temp_data_weather t3 ON t1.weather_id = t3.ID
                                                            LEFT JOIN datawarehouse.dim_province dp ON t2.province = dp.province
                                                            LEFT JOIN datawarehouse.dim_weather dw ON t3.weather = dw.weather
                                                        ORDER BY
                                                            t1.ID;
                                        `);
                                        // 24. Check result of query Insert temp_data_province, temp_data_weather, temp_data_main_table into
                                        // to table dim_province, table dim_weather, table fact_main_table are true
                                        if (insertToDim_Province && insertToDim_Weather && insertToFact_Main_Table){
                                            console.log("Chuyển sang datawarehouse thành công");
                                            // 27.Update Table log(control):time:now, process_id: 4, status:successful
                                            const queryLogSuccess =
                                                "UPDATE control.log SET time = NOW(), status = ? WHERE id = (SELECT id FROM control.log WHERE process_id = 4 AND status = 'start' AND DATE_FORMAT(time, '%Y-%m-%d %H') = DATE_FORMAT(NOW(), '%Y-%m-%d %H'))";
                                            const status = "successful";
                                            const valuesLogSuccess = [status];
                                            connectionControl.query(queryLogSuccess, valuesLogSuccess, (error, results) => {
                                                if (error) throw error;
                                                console.log("Update log process: 4");
                                            });
                                            // 28. Close connect toDB datawarehouse, DB staging, DB control
                                            await connectionDatawarehouse.end();
                                            await connectionStaging.end();
                                            await connectionControl.end();
                                        }else {
                                            console.log("Chuyển sang datawarehouse thất bại");
                                            // 25.Update Table log(control):time:now, process_id:4,status:failed
                                            const queryLogFail =
                                                "UPDATE control.log SET time = NOW(), status = ? WHERE id = (SELECT id FROM control.log WHERE process_id = 4 AND status = 'start' AND DATE_FORMAT(time, '%Y-%m-%d %H') = DATE_FORMAT(NOW(), '%Y-%m-%d %H'))";
                                            const status = "failed";
                                            const valuesLogFail = [status];
                                            connectionControl.query(queryLogFail, valuesLogFail, (error, results) => {
                                                if (error) throw error;
                                                console.log("Update log process: 4");
                                            });
                                            // 26. Close connect toDB datawarehouse, DB staging, DB control
                                            await connectionDatawarehouse.end();
                                            await connectionStaging.end();
                                            await connectionControl.end();
                                        }

                                        // Đóng kết nối Datamart
                                        await connectionDatawarehouse.end();
                                    } catch (datawarehouseError) {
                                        console.error('Lỗi trong quá trình làm việc với DataWarehouse:', datawarehouseError);
                                        // Xử lý lỗi khi kết nối đến Datawarehouse thất bại
                                        // 21.Update Table log(control):time:now, process_id:4,status:failed
                                        const queryLogFail =
                                            "UPDATE control.log SET time = NOW(), status = ? WHERE id = (SELECT id FROM control.log WHERE process_id = 4 AND status = 'start' AND DATE_FORMAT(time, '%Y-%m-%d %H') = DATE_FORMAT(NOW(), '%Y-%m-%d %H'))";
                                        const status = "failed";
                                        const valuesLogFail = [status];
                                        connectionControl.query(queryLogFail, valuesLogFail, (error, results) => {
                                            if (error) throw error;
                                            console.log("Update log process: 4");
                                        });
                                        // 22.Close connect to DB staging, DB control
                                        await connectionStaging.end();
                                        await connectionControl.end();
                                    }
                                }else {
                                    console.log("Chuyển sang bộ nhớ ảo thất bại");
                                    // 18.Update Table log(control):time:now, process_id:4,status:failed
                                    const queryLogFail =
                                        "UPDATE control.log SET time = NOW(), status = ? WHERE id = (SELECT id FROM control.log WHERE process_id = 4 AND status = 'start' AND DATE_FORMAT(time, '%Y-%m-%d %H') = DATE_FORMAT(NOW(), '%Y-%m-%d %H'))";
                                    const status = "failed";
                                    const valuesLogFail = [status];
                                    connectionControl.query(queryLogFail, valuesLogFail, (error, results) => {
                                        if (error) throw error;
                                        console.log("Update log process: 4");
                                    });
                                    // 19. Close connect to DB staging, DB control
                                    await connectionStaging.end();
                                    await connectionControl.end();
                                }
                            }else {
                                console.log("Tạo bảng tạm thất bại");
                                // 14.Update Table log(control):time:now, process_id:4,status:failed
                                const queryLogFail =
                                    "UPDATE control.log SET time = NOW(), status = ? WHERE id = (SELECT id FROM control.log WHERE process_id = 4 AND status = 'start' AND DATE_FORMAT(time, '%Y-%m-%d %H') = DATE_FORMAT(NOW(), '%Y-%m-%d %H'))";
                                const status = "failed";
                                const valuesLogFail = [status];
                                connectionControl.query(queryLogFail, valuesLogFail, (error, results) => {
                                    if (error) throw error;
                                    console.log("Update log process: 4");
                                });
                                // 15. Close connect to DB staging, DB control
                                await connectionStaging.end();
                                await connectionControl.end();
                            }

                        } catch (stagingError) {
                            console.error('Lỗi trong quá trình làm việc với Staging:', stagingError);
                            // Xử lý lỗi khi kết nối đến Datamart thất bại
                            // 10.Update Table log(control):time:now, process_id:4,status:failed
                            const queryLogFail =
                                "UPDATE control.log SET time = NOW(), status = ? WHERE id = (SELECT id FROM control.log WHERE process_id = 4 AND status = 'start' AND DATE_FORMAT(time, '%Y-%m-%d %H') = DATE_FORMAT(NOW(), '%Y-%m-%d %H'))";
                            const status = "failed";
                            const valuesLogFail = [status];
                            connectionControl.query(queryLogFail, valuesLogFail, (error, results) => {
                                if (error) throw error;
                                console.log("Update log process: 4");
                            });
                            // 11. Close connect to DB control
                            await connectionControl.end();
                        }
                    } else {
                        console.log('Chương trình load_main đã thực hiện thành công');
                        // 7. Close connect to DB control
                        await connectionControl.end();
                    }
                } else {
                    console.log('Đang có chương trình load_main đang chạy');
                    // 5. Close connect to DB control
                    await connectionControl.end();
                }
            } else {
                console.log('Chương trình load data to staging chưa hoàn thành');
                // 3. Close connect to DB control
                await connectionControl.end();
            }

        } catch (error) {
            console.error('Lỗi trong quá trình kiểm tra dữ liệu:', error);
        }
    } catch (error) {
        console.error('Lỗi trong quá trình kết nối:', error);
    }
}

// Gọi hàm main để bắt đầu quá trình kiểm tra dữ liệu
main();