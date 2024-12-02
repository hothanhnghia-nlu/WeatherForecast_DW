const mysql = require('mysql2');
// Import thư viện axios
const axios = require('axios');
// Import thư viện fs để đọc file
const fs = require('fs');
const cheerio = require('cheerio');
const { title } = require('process');
const readline = require('readline');
const path = require('path');


function nameFile1() {
    const now = new Date();
    const hours = now.getHours();

    const currentTime = `${hours}-thoitiet`;
    return currentTime;
}

function nameFile2() {
    const now = new Date();
    const hours = now.getHours();

    const currentTime = `${hours}-thoitietedu`;
    return currentTime;
}

function nameFolder() {
    const now = new Date();
    const year = now.getFullYear();
    const month = now.getMonth() + 1; // Lưu ý: Tháng trong JavaScript bắt đầu từ 0, nên cần +1 để đúng tháng hiện tại.
    const day = now.getDate();

    const currentTime = `${day}-${month}-${year}`;
    return currentTime;
}

// Hàm đọc file cấu hình
function readConfigFile(configFilePath) {
    try {
        // Đọc nội dung file cấu hình
        const configFileContent = fs.readFileSync(configFilePath, 'utf8');
        // Parse nội dung file thành đối tượng JSON
        const configData = JSON.parse(configFileContent);
        return configData;
    } catch (error) {
        console.error('Lỗi khi đọc file cấu hình:', error);
        return null;
    }
}

// Lấy thời gian hiện tại
function getTime() {
    const now = new Date();
    const year = now.getFullYear();
    const month = now.getMonth() + 1; // Lưu ý: Tháng trong JavaScript bắt đầu từ 0, nên cần +1 để đúng tháng hiện tại.
    const day = now.getDate();
    const hours = now.getHours();
    const minutes = now.getMinutes();
    const seconds = now.getSeconds();

    const currentTime = `${hours}:${minutes}:${seconds} ${day}:${month}:${year}`;
    return currentTime;
}

function kiemTra(listUrl, url) {
    // Sử dụng phương thức includes để kiểm tra xem có giá trị "a" trong mảng hay không
    return listUrl.includes(url);
}

// Hàm chính
async function main() {

    // Đường dẫn tới file cấu hình
    const configFilePath = 'config.json';

    // 1. Read all info from config.json in same folder with file hh-thoitietedu.js
    const configData = readConfigFile(configFilePath);
    if (!configData) {
        console.error('Không thể đọc file cấu hình. Dừng chương trình.');
        return;
    }

    // 2. Connect DB: control
    const connection = mysql.createConnection({
        host: configData.host,
        user: configData.user,
        password: configData.password,
        database: configData.database
    });

    connection.connect((err) => {
        if (err) {
            console.error('Lỗi kết nối DB control:', err);
            return;
        }else{
            console.error('Kết nối DB control thành công');
            var url1 = 'https://thoitiet.vn';
            var url2 = 'https://thoitiet.edu.vn/';
            var length_list_url = configData.list_url.length;
            //3. Check if the file named in the list of allowed data loading is successful or not in now hour 
            if(length_list_url==0){
                console.log('Close connect DB');
                //4. Close connect DB: control
                connection.end();
            }else{
                var now = new Date();
                var hour = now.getHours();
                var day = now.getDate();
                var month = now.getMonth() + 1;
                var year = now.getFullYear();

                var countQuery = 'SELECT COUNT(id) as count FROM log WHERE process_id = 3 AND status != "failed" AND HOUR(time) = ? AND DAYOFMONTH(time) = ? AND MONTH(time) = ? AND YEAR(time) = ?';

                // Thực hiện câu truy vấn COUNT
                connection.query(countQuery, [hour, day, month, year], function (error, results) {
                        if (error) {
                            console.error('Lỗi truy vấn COUNT:', error.message);
                            connection.end();
                            return;
                        }

                        var countResult = results[0].count;
                        //3. Check if the file named in the list of allowed data loading is successful or not in now hour
                        if(countResult!=0){
                            //4. Close connect DB: control
                            connection.end();
                            return;
                        }
                        // Kiểm tra nếu count bằng 0 thì thực hiện câu truy vấn chính
                        var querycheckLog = 'SELECT COUNT(id) as count FROM log WHERE ';
                        var params = [];
                        var conditions = [];

                        if (kiemTra(configData.list_url, url1)) {
                            conditions.push('(process_id = 1 AND status = "successful" AND HOUR(time) = ? AND DAYOFMONTH(time) = ? AND MONTH(time) = ? AND YEAR(time) = ?)');
                            params.push(hour, day, month, year);
                        }
                        if (kiemTra(configData.list_url, url2)) {
                            conditions.push('(process_id = 2 AND status = "successful" AND HOUR(time) = ? AND DAYOFMONTH(time) = ? AND MONTH(time) = ? AND YEAR(time) = ?)');
                            params.push(hour, day, month, year);
                        }

                        if (conditions.length > 0) {
                            querycheckLog += conditions.join(' OR ');
                        }

                        //3. Check if the file named in the list of allowed data loading is successful or not in now hour
                        connection.query(querycheckLog, params, function (err, result) {
                            if(err) throw err;
                            rowCount3 = result[0].count;
                            console.log('rowCount3: '+rowCount3);

                            if(rowCount3==0){
                                // 4. Close connect DB: control
                                connection.end();
                            }else{
                                // 5. Insert Table log(control):time:now, process:3,status:start 
                                const queryLog = 'INSERT INTO log (time, process_id, status) VALUES ?';
                                var time = new Date();
                                var process = '3';
                                var statuss = 'start';
                                const valueLog = [[time, process, statuss]];

                                connection.query(queryLog, [valueLog], function(err, result) {
                                    if (err) throw err;
                                    console.log('Insert log start process 3');
                                });

                                // Create 1 variable folder_data_path and set variable from query: select folder_data_path from config where process_id = 2 and YEAR(ee_date) = 9999
                                var folder_data_path = '';

                                var sql = "SELECT src_path, dest FROM config WHERE process_id = 2 and YEAR(ee_date) = 9999";
                                connection.query(sql, function(err, result) {
                                    if (err) throw err;

                                    // Lấy giá trị url và folder_data_path
                                    folder_data_path = result[0].dest

                                    // 6. Connect DB: staging
                                    const connection2 = mysql.createConnection({
                                        host: configData.host,
                                        user: configData.user,
                                        password: configData.password,
                                        database: configData.databasestaging
                                    });

                                    connection2.connect((err) =>{
                                        if (err) {
                                            console.error('Lỗi kết nối DB staging:', err);
                                            // 7. Close connect DB: control
                                            connection.end();
                                            return;
                                        }else{
                                            // 8. Write SQL command to delete all data in second table
                                            const querytruncate = 'TRUNCATE TABLE `second_table`';
                                            // 9. Run SQL command
                                            connection2.query(querytruncate,[],function(err, result){
                                                if (err){
                                                    // 10. Update Table log(control): time:now, status: failed
                                                    var time = new Date();
                                                    var staus = 'failed';
                                                    var status = 'start';
                                                    var hour = time.getHours();
                                                    var date = time.getDate();
                                                    var month = time.getMonth()+1;
                                                    var year = time.getFullYear();
                                                    var sql = `UPDATE log SET time = ?, status = ? WHERE id = (SELECT id FROM log WHERE process_id = 3 AND status = ? and HOUR(time) = ? and DAYOFMONTH(time) = ? and MONTH(time) = ? and YEAR(time) = ? )`;

                                                    connection.query(sql, [time, staus, status, hour, date, month, year], function(err, result) {
                                                        if (err) throw err;
                                                        console.log(`Đã cập nhật ${result.affectedRows} hàng`);
                                                        // 11. Close connect DB: control
                                                        connection.end();
                                                        return;
                                                    });
                                                }else{
                                                    // 12. Create variable to assign path json file data
                                                    var newestFileUrl1;
                                                    var newestFileUrl2;

                                                    const folder = folder_data_path;
                                                    const subFolder = `${nameFolder()}`;
                                                    const FilePath1 = `${nameFile1()}.json`;
                                                    const outputFilePath1 = path.join(folder, subFolder, FilePath1);
                                                    console.log(outputFilePath1);

                                                    const FilePath2 = `${nameFile2()}.json`;
                                                    const outputFilePath2 = path.join(folder, subFolder, FilePath2);
                                                    console.log(outputFilePath2);

                                                    newestFileUrl1 = outputFilePath1;
                                                    newestFileUrl2 = outputFilePath2;

                                                    var time = getTime();

                                                    if(kiemTra(configData.list_url, url1)&&kiemTra(configData.list_url, url2)){
                                                        console.log('url 1&2');
                                                        const jsonData1 = JSON.parse(fs.readFileSync(newestFileUrl1, 'utf8'));
                                                        const jsonData2 = JSON.parse(fs.readFileSync(newestFileUrl2, 'utf8'));
                                                        var values1 = jsonData1.map(item => [time, item.province, item.temperature, item.weather, item.humidity, item.T[0], item.T[1], item.T[2], item.T[3], item.T[4], url1]);
                                                        var values2 = jsonData2.map(item => [time, item.province, item.temperature, item.weather, item.humidity, item.T[0], item.T[1], item.T[2], item.T[3], item.T[4], url2]);
                                                        var values = values1.concat(values2);
                                                    }else if(kiemTra(configData.list_url, url1)){
                                                        console.log('url 1');
                                                        const jsonData1 = JSON.parse(fs.readFileSync(newestFileUrl1, 'utf8'));
                                                        var values = jsonData1.map(item => [time, item.province, item.temperature, item.weather, item.humidity, item.T[0], item.T[1], item.T[2], item.T[3], item.T[4], url1]);
                                                    }else if(kiemTra(configData.list_url, url2)){
                                                        console.log('url 2');
                                                        const jsonData2 = JSON.parse(fs.readFileSync(newestFileUrl2, 'utf8'));
                                                        var values = jsonData2.map(item => [time, item.province, item.temperature, item.weather, item.humidity, item.T[0], item.T[1], item.T[2], item.T[3], item.T[4], url2]);
                                                    }

                                                    // 13. Insert json data from file json to second-table in DB staging
                                                    const query = 'INSERT INTO second_table (time, province, temperature, weather, humidity, t1, t2, t3, t4, t5, source) VALUES ?';

                                                    connection2.query(query, [values], (err, result) => {
                                                        if (err) {
                                                            console.error('Lỗi thêm dữ liệu:', err);
                                                            // 14. Update Table log(control):time:now, status:failed 
                                                            var time = new Date();
                                                            var staus = 'failed';
                                                            var status = 'start';
                                                            var hour = time.getHours();
                                                            var date = time.getDate();
                                                            var month = time.getMonth()+1;
                                                            var year = time.getFullYear();
                                                            var sql = `UPDATE log SET time = ?, status = ? WHERE id = (SELECT id FROM log WHERE process_id = 3 AND status = ? and HOUR(time) = ? and DAYOFMONTH(time) = ? and MONTH(time) = ? and YEAR(time) = ? )`;

                                                            connection.query(sql, [time, staus, status, hour, date, month, year], function(err, result) {
                                                                if (err) throw err;
                                                            })
                                                            // 15. Close connect DB: control, staging
                                                            connection.end();
                                                            connection2.end();
                                                            return;
                                                        }
                                                        console.log(`${result.affectedRows} bản ghi đã được thêm vào cơ sở dữ liệu`);

                                                        // 16. Update Table log(control):time:now, status:successful 
                                                        var time = new Date();
                                                        var staus = 'successful';
                                                        var status = 'start';
                                                        var hour = time.getHours();
                                                        var date = time.getDate();
                                                        var month = time.getMonth()+1;
                                                        var year = time.getFullYear();
                                                        var sql = `UPDATE log SET time = ?, status = ? WHERE id = (SELECT id FROM log WHERE process_id = 3 AND status = ? and HOUR(time) = ? and DAYOFMONTH(time) = ? and MONTH(time) = ? and YEAR(time) = ? )`;

                                                        connection.query(sql, [time, staus, status, hour, date, month, year], function(err, result) {
                                                            if (err) throw err;
                                                            console.log(`Đã cập nhật ${result.affectedRows} hàng`);
                                                            console.log('Record inserted:', result);
                                                        })
                                                        // 17. Close connect DB: control, staging
                                                        connection2.end();
                                                        connection.end();
                                                    });
                                                };
                                            })
                                        }
                                    })
                                })}
                        })

                    }
                )
            }}})}

// Chạy chương trình chính
main();
