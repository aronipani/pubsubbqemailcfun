/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
exports.alertEmailFun = (event, context) => {
  const message = event.data
    ? Buffer.from(event.data, 'base64').toString()
    : 'No Data Found';
  console.log(message);
  const obj = JSON.parse(message);
  maximum_water_height = parseFloat(obj.maximum_water_height);

  console.log(obj.maximum_water_height + " : : " + maximum_water_height);

  queryTelemetry().then(rows =>{
    console.log(`In Calling function ${rows} `);
    rows.forEach(
      row => {
        //console.log(`maximum_water_height : ${row.maximum_water_height}: alert_flag : ${row.alert_flag}`);
        console.log(`assigning row data .. ${row}`)
        let maximum_water_height_bq = row.maximum_water_height;
        let alert_flag = row.alert_flag;
        console.log(`maximum_water_height_bq : ${maximum_water_height_bq}: alert_flag  : ${alert_flag}`);
        console.log(`maximum_water_height = ${maximum_water_height}`)
        if (maximum_water_height >= maximum_water_height_bq ){
            console.log(`Trigger Email ....${maximum_water_height} >= ${maximum_water_height_bq}`);
            sendEmail(maximum_water_height)
        }
        else
          console.log(`All OK no Email ....${maximum_water_height} < ${maximum_water_height_bq}`);
      }

    );

  }).catch(err => {
    console.log("Error : " + err);
  });;

  //console.log(`In Calling function pass 1 .. ${tmd}`)
};


const {BigQuery} = require('@google-cloud/bigquery');

async function queryTelemetry() {
      // Queries a public GitHub dataset.

      // Create a client
      const bigqueryClient = new BigQuery();

      // The SQL query to run
      const sqlQuery = `SELECT maximum_water_height , alert_flag
      FROM \`manning-data-pipelines.tsunami.tsunami_alert\`
      `;
      console.log("In BQ")
      const options = {
      query: sqlQuery,
      // Location must match that of the dataset(s) referenced in the query.
      location: 'us-central1',
      };

      // Run the query
      const [rows] = await bigqueryClient.query(options);

      //console.log('Rows:');
      //rows.forEach(row => console.log(`${row.maximum_water_height}: ${row.alert_flag}`));
      return rows;
  }
  
async function sendEmail(data) {
  const sgMail = require('@sendgrid/mail');
  console.log(`process.env ${process.env}`);
  console.log(`process.env.SENDGRID_API_KEY ${process.env.SENDGRID_API_KEY}`);
  sgMail.setApiKey(process.env.SENDGRID_API_KEY);
  //sgMail.setApiKey('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');
  const msg = {
    to: 'paniaroni@gmail.com',
    from: process.env.from_email, // Use the email address or domain you verified above
    subject: 'Alert Email for Water Height',
    text: 'There is an Alert with the Water Height',
    html: `<strong>Alert: ${data}</strong>`,
  }
  sgMail
  .send(msg)
  .then(() => {
    console.log('Email sent')
  })
  .catch((error) => {
    console.error(error)
  })

}