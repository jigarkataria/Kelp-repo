var express = require('express');
var router = express.Router();
const csvFilePath = '/Users/jigarkataria/Documents/project/public/csv-file/convertcsv.csv'; // change file name accordinly
const csv = require('csvtojson');
const { Pool } = require('pg');

// Database connection details
const pool = new Pool({
  user: 'your_username',
  host: 'your_database_host',
  database: 'your_database_name',
  password: 'your_password',
  port: 5432
});

router.get('/', async function (req, res, next) {
  const jsonArray = await csv().fromFile(csvFilePath);
  // Map CSV columns to table fields
  const fieldMapping = {
    'name.firstName': 'first_name',
    'name.lastName': 'last_name',
    'age': 'age',
    'address.line1': 'address_line1',
    'address.line2': 'address_line2',
    'address.city': 'address_city',
    'address.state': 'address_state',
    'gender': 'gender',
    'name' : 'name', // to remove from additional info
    'address' : 'address' // to remove from additional info
  };
  let results = [];
  for (let i = 0; i < jsonArray.length; i++) {
    const record = {};
    let nameRecord = jsonArray[i]
    // Map CSV fields to table fields
    for (const csvField in fieldMapping) {
      const tableField = fieldMapping[csvField];
      if (csvField.includes('.')) {
        let field = csvField.split('.')
        record[tableField] = nameRecord[field[0]][field[1]];
      } else {
        record[tableField] = nameRecord[csvField];
      }
    }
    const additionalInfo = {};
    //Additional Info
    for (const field in jsonArray[i]) {
      if (!fieldMapping.hasOwnProperty(field)) {
        additionalInfo[field] = jsonArray[i][field];
      }
    }
    record.additional_info = additionalInfo;   
    results.push(record);
    if((results.length % 100) == 0){
      insertInDB(results);
      results = [];
    }
  }
  res.send('Reading File in Progress');
});

const insertInDB = async (records) => {
  const insertPromises = records.map((record) => {
    const { first_name, last_name, age, address, additional_info } = record;
    const name = `${first_name} ${last_name}`;
    return pool.query(
      'INSERT INTO public.users (name, age, address, additional_info) VALUES ($1, $2, $3, $4)',
      [name, age, address, additional_info]
    );
  });

  Promise.all(insertPromises)
    .then(() => {
      calculateAgeDistribution();
      console.log('Data inserted successfully');
    })
    .catch((error) => {
      console.error('Error inserting data:', error);
    });
}

const calculateAgeDistribution = async () => {
  try {
    const client = await pool.connect();
    const result = await client.query('SELECT age, count(*) as count FROM public.users GROUP BY age');

    console.log('Age-Group % Distribution');
    console.log('< 20', Math.floor(result.rows.find(row => row.age < 20).count / result.rowCount * 100));
    console.log('20 to 40', Math.floor(result.rows.find(row => row.age >= 20 && row.age <= 40).count / result.rowCount * 100));
    console.log('40 to 60', Math.floor(result.rows.find(row => row.age > 40 && row.age <= 60).count / result.rowCount * 100));
    console.log('> 60', Math.floor(result.rows.find(row => row.age > 60).count / result.rowCount * 100));

    client.release();
  } catch (error) {
    console.error('Error occurred during age distribution calculation:', error);
  }
};

calculateAgeDistribution();

module.exports = router;
