const mysql = require('mysql2');
// Import thư viện axios
const axios = require('axios');
// Import thư viện fs để đọc file
const fs = require('fs');
const cheerio = require('cheerio');
const { title } = require('process');
const readline = require('readline');
const path = require('path');

// Hàm lấy dữ liệu từ web
async function fetchDataFromWeb(url) {
    try {
        const response = await axios.get(url);
        const html = response.data;
        // Sử dụng cheerio để phân tích mã HTML
        const $ = cheerio.load(html);

        // Lấy nội dung của phần tử có id hoặc class tương ứng
        const list_url = $('ul.mega-submenu li a').map(function () {
            return url+$(this).attr('href')+'/10-ngay-toi';
        }).get();

        const promises = list_url.map(href => {
            return axios.get(href)
                .then(response => {
                    const $ = cheerio.load(response.data);
                    var temp1 = $('span.summary-temperature-max-value').eq(1).text().trim().replace("°C", "");
                    var temp2 = $('span.summary-temperature-max-value').eq(2).text().trim().replace("°C", "");
                    var temp3 = $('span.summary-temperature-max-value').eq(3).text().trim().replace("°C", "");
                    var temp4 = $('span.summary-temperature-max-value').eq(4).text().trim().replace("°C", "");
                    var temp5 = $('span.summary-temperature-max-value').eq(5).text().trim().replace("°C", "");
                    const temp = $('span.current-temperature').text().replace("°", "");
                    const temperature = Math.round(temp);
                    const weather = $('p.overview-caption-item:first').text();
                    const humidity = $('h3.text-white.op-8.fw-bold').eq(1).text().replace("%", "");
                    var temp_bright = [];
                    temp_bright.push(Math.round(temp1));
                    temp_bright.push(Math.round(temp2));
                    temp_bright.push(Math.round(temp3));
                    temp_bright.push(Math.round(temp4));
                    temp_bright.push(Math.round(temp5));
                    const data = {
                        province: $('span.location-name-main a').text().trim(),
                        temperature: temperature,
                        weather: weather,
                        humidity: humidity,
                        T: temp_bright
                    }
                    return data;
                })
                .catch(error => {
                    console.log("Yêu cầu không thành công:", error.message);
                    return null;
                });
        });

        const data = await Promise.all(promises);
        return data;
    } catch (error) {
        console.error('Lỗi khi lấy dữ liệu từ web:', error);
        return null;
    }
}

// Hàm lưu dữ liệu vào file
function saveDataToFile(data, filePath) {
    try {
        // Chuyển đổi dữ liệu thành chuỗi JSON
        const updatedData = JSON.stringify(data, null, 2);
        fs.writeFileSync(filePath, updatedData, 'utf8');
        console.log('Dữ liệu đã được lưu vào file thành công.');
        return true;
    } catch (error) {
        console.error('Lỗi khi lưu dữ liệu vào file:', error);
        return false;
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

    const currentTime = `${hours}:${minutes}:${seconds} ${day}/${month}/${year}`;
    return currentTime;
}

function nameFile() {
    const now = new Date();
    const hours = now.getHours();

    const currentTime = `${hours}-thoitiet`;
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

function kiemTra(listUrl) {
    // Sử dụng phương thức includes để kiểm tra xem có giá trị "a" trong mảng hay không
    return listUrl.includes("https://thoitiet.vn");
}

// Hàm chính
async function main() {

    // Đường dẫn tới file cấu hình
    const configFilePath = 'config.json';

    // 1. Read all info from config.json in same folder with file hh-thoitiet.js
    const configData = readConfigFile(configFilePath);
    if (!configData) {
        console.error('Không thể đọc file cấu hình. Dừng chương trình.');
        return;
    }

    //2. Check if the file is in the list of allowed data to load
    var check = kiemTra(configData.list_url);
    if(check){
        console.log('Có tên trong danh sách file cần lấy dữ liệu.');
    }else{
        console.log('Không có trong danh sách file cần lấy dữ liệu.');
        return;
    }

    // 3. Connect DB: control
    const connection = mysql.createConnection({
        host: configData.host,
        user: configData.user,
        password: configData.password,
        database: configData.database
    });

    connection.connect((err) => {
        if (err) {
            console.error('Lỗi kết nối MySQL:', err);
            return;
        }else{
            console.log("OK");
            const querycheckLog = 'SELECT COUNT(id) as count FROM log WHERE status != ? and process_id = ? and HOUR(time) = ? and DAYOFMONTH(time) = ? and MONTH(time) = ? and YEAR(time) = ?';
            var rowCount = 0;
            var status = 'failed';
            var process = '1';
            var time = new Date();
            var hour = time.getHours();
            var day = time.getDate();
            var month = time.getMonth()+1;
            var year = time.getFullYear();

            // 4. Check process not exist log successful in now hour
            connection.query(querycheckLog, [status, process, hour, day, month, year], function(err, result) {
                if (err) throw err;
                rowCount = result[0].count;

                if(rowCount!=0){
                    // 5. Close connect DB: control
                    connection.end();
                }else{
                    // 6. Insert Table log(control):time:now, process:1,status:start 
                    const queryLog = 'INSERT INTO log (time, process_id, status) VALUES ?';
                    var time = new Date();
                    var process = '1';
                    var status = 'start';
                    var valueLog = [[time, process, status]];

                    connection.query(queryLog, [valueLog], function(err, result) {
                        if (err) throw err;
                        console.log('Insert log start process 1');
                    });

                    // 7. Create 2 variable url and folder_data_path and set variable from query: select url, folder_data_path from config where process_id = 1 and YEAR(ee_date) = 9999
                    var url = '';
                    var folder_data_path = '';

                    var sql = "SELECT src_path, dest FROM config WHERE process_id = 1 and YEAR(ee_date) = 9999";
                    connection.query(sql, function(err, result) {
                        if (err) throw err;

                        // Lấy giá trị url và folder_data_path
                        url = result[0].src_path;
                        folder_data_path = result[0].dest;
                        // 8.Create variable data and Read the html source code from hhttps://thoitiet.vn

                        var count = 0;
                        fetchDataFromWeb(url).then(data => {
                            console.log('Dữ liệu từ web:', data);
                            count++;
                            // 9. Update Table log(control): time:now, status: failed
                            if(data == null){
                                var time = new Date();
                                var staus = 'failed';
                                var status = 'start';
                                var hour = time.getHours();
                                var date = time.getDate();
                                var month = time.getMonth()+1;
                                var year = time.getFullYear();
                                var sql = `UPDATE log SET time = ?, status = ? WHERE id = (SELECT id FROM log WHERE process_id = 1 AND status = ? and HOUR(time) = ? and DAYOFMONTH(time) = ? and MONTH(time) = ? and YEAR(time) = ? )`;

                                connection.query(sql, [time, staus, status, hour, date, month, year], function(err, result) {
                                    if (err) throw err;
                                    console.log(`Đã cập nhật ${result.affectedRows} hàng`);
                                    //10. Close connect DB: control
                                    connection.end();
                                });
                            }else{
                                const folder = folder_data_path;
                                const subFolder = `${nameFolder()}`;
                                const FilePath = `${nameFile()}.json`;
                                const folderPath = path.join(folder,subFolder);
                                // 11. Check folder YY-MM-DD in folder_data_path exist
                                if (!fs.existsSync(folderPath)) {
                                    // 12. Create folder YY-MM-DD in folder_data_path
                                    fs.mkdirSync(folderPath);
                                    console.log('Đã tạo thư mục mới!');
                                } else {
                                    console.log('Thư mục đã tồn tại!');
                                }
                                const outputFilePath = path.join(folder, subFolder, FilePath);

                                // 13. Save data in folder YY-MM-DD with name hh-thoitet.json
                                var check = saveDataToFile(data, outputFilePath);
                                if(check){
                                    // 16. Update Table log(control): time:now, status: successful
                                    var time = new Date();
                                    var staus = 'successful';
                                    var status = 'start';
                                    var hour = time.getHours();
                                    var date = time.getDate();
                                    var month = time.getMonth()+1;
                                    var year = time.getFullYear();
                                    var sql = `UPDATE log SET time = ?, status = ? WHERE id = (SELECT id FROM log WHERE process_id = 1 AND status = ? and HOUR(time) = ? and DAYOFMONTH(time) = ? and MONTH(time) = ? and YEAR(time) = ? )`;

                                    connection.query(sql, [time, staus, status, hour, date, month, year], function(err, result) {
                                        if (err) throw err;
                                        console.log(`Đã cập nhật ${result.affectedRows} hàng`);
                                        // 17. Close connect DB: control
                                        connection.end();
                                    });
                                }else{
                                    // 14. Update Table log(control): time:now, status: failed
                                    var time = new Date();
                                    var staus = 'failed';
                                    var status = 'start';
                                    var hour = time.getHours();
                                    var date = time.getDate();
                                    var month = time.getMonth()+1;
                                    var year = time.getFullYear();
                                    var sql = `UPDATE log SET time = ?, status = ? WHERE id = (SELECT id FROM log WHERE process_id = 1 AND status = ? and HOUR(time) = ? and DAYOFMONTH(time) = ? and MONTH(time) = ? and YEAR(time) = ? )`;

                                    connection.query(sql, [time, staus, status, hour, date, month, year], function(err, result) {
                                        if (err) throw err;
                                        console.log(`Đã cập nhật ${result.affectedRows} hàng`);
                                        // 15. Close connect DB: control
                                        connection.end();
                                    });
                                }

                            }

                        });
                    })
                }})

        }
    });

}

// Chạy chương trình chính
main();
