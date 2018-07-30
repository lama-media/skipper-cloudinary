const Writable = require('stream').Writable;

module.exports = function CloudinaryReceiver(cloudinary, options) {

  const receiver = Writable({
    objectMode: true
  });

  options.rowHandler = options.rowHandler || function () {
    };

  receiver._files = [];

  receiver._write = function onFile(file, encoding, done) {

    const headers = options.headers || {};
    const localID = _.uniqueId();
    const __newFile = file;

    const stream = cloudinary.v2.uploader.upload_stream(options.uploadOptions, function (error, result) {

      if (error) {
        return receiver.emit('error', error.message);
      }

      file.extra     = result;
      file.byteCount = result.bytes;
      file.size      = result.bytes;
      done();
    });

    stream.on('error', function (error) {
      done(error);
    });

    stream.on('data', function (chunk) {
      var currentFileProgress = _.find(receiver._files, {
        id: localID
      });

      if (currentFileProgress) {
        currentFileProgress.written += chunk.length;
        currentFileProgress.total = __newFile.byteCount;
        currentFileProgress.percent = Math.round(currentFileProgress.written/__newFile.byteCount*100);
        currentFileProgress.stream = __newFile;
      } else {
        currentFileProgress = {
          id: localID,
          fd: __newFile.fd,
          name: __newFile.filename,
          written: 0,
          total: __newFile.byteCount,
          percent: 0,
          stream: __newFile
        };
        receiver._files.push(currentFileProgress);
      }
      receiver.emit('progress', currentFileProgress);
    });

    stream.on('readable', function () {
      let record;
      while (record = stream.read()) {
        options.rowHandler(record, file.fd, file);
      }
    });

    file.pipe(stream);
  };

  return receiver;
};
