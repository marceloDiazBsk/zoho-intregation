import fs from 'fs';
import moment from "moment";

function test(){
    const response = fs.readFileSync('./leads.json').toString();
    const leadList = JSON.parse(response);
    console.log('leadList.length',leadList.length);
}

test();