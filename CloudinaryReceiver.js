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
    var sent = 0;

    options.uploadOptions.stream = true;
    options.uploadOptions.chunk_size = 5242880;

    const stream = cloudinary.uploader.upload_large_stream(null, function (result) {

      if (result.error) {
        return receiver.emit('error', result.error.message);
      }

      file.extra     = result;
      file.byteCount = result.bytes;
      file.size      = result.bytes;
      done();
    }, options.uploadOptions);

    stream.on('ready', function(buffer, is_last, done) {
      var content_range = "bytes " + chunk_start + "-" + (sent - 1) + "/" + (is_last ? sent : -1);
      var chunk_start = sent;
      sent += buffer.length;
      // console.log( "bytes " + chunk_start + "-" + (sent - 1) + "/" + (is_last ? sent : -1) )

      var currentFileProgress = _.find(receiver._files, {
        id: localID
      });

      if (currentFileProgress) {
        currentFileProgress.written += buffer.length;
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

    })

    stream.on('error', function (error) {
      done(error);
    });


    file.pipe(stream);
  };

  return receiver;
};
