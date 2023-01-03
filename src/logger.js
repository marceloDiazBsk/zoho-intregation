import moment from "moment";
import log4js from "log4js";

log4js.configure({
  appenders: {
    everything: {
      type: "file",
      filename: "./logs/" + moment().format('YYYY-MM-DD')  +".log",
      maxLogSize: 10485760,
      backups: 3,
      compress: true,
    },
  },
  categories: {
    default: { appenders: ["everything"], level: "info" },
  },
});
const logger = log4js.getLogger();

export default logger;