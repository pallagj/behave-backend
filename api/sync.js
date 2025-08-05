import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore } from 'firebase-admin/firestore';
import fetch from 'node-fetch';
import cheerio from 'cheerio';

/**
 * Parses the HTML from the monitoring page using Cheerio.
 * @param {string} htmlString - The raw HTML of the page.
 * @returns {Array<Object>} An array of measurement data objects.
 */
const parseHtmlData = (htmlString) => {
    const $ = cheerio.load(htmlString);
    const data = [];
    // Find the last table on the page and iterate over its rows
    $('table:last-of-type tr').each((i, row) => {
        if (i < 2) return; // Skip the two header rows

        const cells = $(row).find('td');
        if (cells.length === 4) {
            try {
                const dateStr = $(cells[0]).text().trim();
                const weight = parseFloat($(cells[1]).text().trim().replace(',', '.'));
                const battery = parseFloat($(cells[2]).text().trim().replace(',', '.'));
                const temp = parseFloat($(cells[3]).text().trim().replace(',', '.'));

                const dateParts = dateStr.match(/(\d{4})\.(\d{2})\.(\d{2})\. (\d{2}):(\d{2}):(\d{2})/);
                if (dateParts) {
                    const [_, year, month, day, hour, minute, second] = dateParts;
                    // Note: Month is 0-indexed in JavaScript Date
                    const timestamp = new Date(year, month - 1, day, hour, minute, second).getTime();

                    if (!isNaN(weight) && !isNaN(battery) && !isNaN(temp) && !isNaN(timestamp)) {
                        data.push({
                            id: timestamp,
                            date: dateStr,
                            timestamp,
                            weight,
                            battery,
                            temp,
                        });
                    }
                }
            } catch (error) {
                console.error("Error parsing row:", $(row).text(), error);
            }
        }
    });
    return data;
};

/**
 * Vercel Serverless Function to sync beehive data.
 * Triggered by a cron job.
 */
export default async function handler(request, response) {
    try {
        // --- 1. Initialize Firebase Admin SDK ---
        // The service account key is stored as a Vercel environment variable
        const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_KEY);
        const appId = process.env.APP_ID || 'beehive-dashboard';

        // Ensure Firebase is initialized only once
        if (getApps().length === 0) {
            initializeApp({
                credential: cert(serviceAccount)
            });
        }
        const db = getFirestore();

        // --- 2. Fetch remote data ---
        console.log("Fetching data from KaptarGSM...");
        const targetUrl = 'https://www.kaptargsm.hu/scale/J0102466.php';
        const fetchResponse = await fetch(targetUrl);
        if (!fetchResponse.ok) {
            throw new Error(`Failed to fetch data: ${fetchResponse.statusText}`);
        }
        const htmlText = await fetchResponse.text();
        console.log("Data fetched successfully.");

        // --- 3. Parse HTML to get data points ---
        const freshData = parseHtmlData(htmlText);
        if (freshData.length === 0) {
            console.log("No data parsed from HTML.");
            return response.status(200).send("Sync check complete: No data found in source HTML.");
        }
        console.log(`Parsed ${freshData.length} data points from HTML.`);

        // --- 4. Write new data to Firestore ---
        // We use a public collection path that doesn't depend on a user ID
        const collectionPath = `artifacts/${appId}/public_data/beehive_data`;
        const dataCollection = db.collection(collectionPath);
        
        const batch = db.batch();
        let newEntriesCount = 0;

        for (const item of freshData) {
            const docRef = dataCollection.doc(item.id.toString());
            // Check if document already exists to avoid unnecessary writes.
            // For simplicity and to ensure data integrity, we use set with merge.
            // A more optimized way would be to get all doc IDs first.
            batch.set(docRef, item);
            newEntriesCount++; // We assume all fetched data is potentially new
        }

        if (newEntriesCount > 0) {
            console.log(`Committing ${newEntriesCount} items to Firestore...`);
            await batch.commit();
            console.log("Batch commit successful.");
            return response.status(200).json({ message: `Sync successful, committed ${newEntriesCount} items.` });
        } else {
            console.log("No new data to commit.");
            return response.status(200).json({ message: "No new data to sync." });
        }

    } catch (error) {
        console.error("Error during sync:", error);
        return response.status(500).json({ error: error.message });
    }
}
