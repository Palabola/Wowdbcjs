const fs = require('fs');

class Reader {

    constructor(filepath)
    {
        const FIELD_TYPE_UNKNOWN = 0;
        const FIELD_TYPE_INT = 1;
        const FIELD_TYPE_FLOAT = 2;
        const FIELD_TYPE_STRING = 3;

        const DISTINCT_STRINGS_REQUIRED = 5;

        const FIELD_COMPRESSION_NONE = 0;
        const FIELD_COMPRESSION_BITPACKED = 1;
        const FIELD_COMPRESSION_COMMON = 2;
        const FIELD_COMPRESSION_BITPACKED_INDEXED = 3;
        const FIELD_COMPRESSION_BITPACKED_INDEXED_ARRAY = 4;
        const FIELD_COMPRESSION_BITPACKED_SIGNED = 5;  

        this.fileHandle;
        this.fileFormat = '';
        this.fileName = '';
        this.fileSize = 0;

        this.headerSize = 0;
        this.recordCount = 0;
        this.fieldCount = 0;
        this.totalFieldCount = 0;
        this.recordSize = 0;
        this.stringBlockPos = 0;
        this.stringBlockSize = 0;
        this.tableHash = 0;
        this.layoutHash = 0;
        this.timestamp = 0;
        this.build = 0;
        this.minId = 0;
        this.maxId = 0;
        this.locale = 0;
        this.copyBlockPos = 0;
        this.copyBlockSize = 0;
        this.flags = 0;

        this.commonBlockPos = 0;
        this.commonBlockSize = 0;
        this.bitpackedDataPos = 0;
        this.lookupColumnCount = 0;
        this.idBlockSize = 0;
        this.fieldStorageInfoPos = 0;
        this.fieldStorageInfoSize = 0;
        this.palletDataPos = 0;
        this.palletDataSize = 0;
        this.relationshipDataPos = 0;
        this.relationshipDataSize = 0;

        this.sectionCount = 0;
        this.sectionHeaders = [];

        this.idField = -1;

        this.hasEmbeddedStrings = false;
        this.hasIdBlock = false;
        this.hasIdsInIndexBlock = false;

        this.indexBlockPos = 0;
        this.idBlockPos = 0;

        this.recordFormat = [];

        this.idMap = [];
        this.recordOffsets = null;

        this.commonLookup = [];

        // JS Things
        this.file_buffer;

        this.file_pointer = 0;

        // File load

        this.fileHandle = filepath;
   
        fs.open(this.fileHandle, 'r', (err, fd) => {
            if (err) throw err;
            fs.fstat(fd, (err, stat) => {
              if (err) throw err;
              // use stat
              this.fileSize = stat.size;

              // always close the file descriptor!
              fs.close(fd, (err) => {
                if (err) throw err;
              });
            });
          });

         this.get_file_buffer((data)=>{
            this.file_buffer = data;
            this.get_fileFormat(); 

            switch(this.fileFormat)
            {
              case 'WDC2':
              this.openWdc2('');
              break;
            }
         });
    } 


    openWdc2()
    {
        let headerFormat = 'V9x/v2y/V7z';

        this.file_pointer = 4;

        this.recordCount = this.get_int_value(this.file_pointer,'uint32');
        this.fieldCount = this.get_int_value(this.file_pointer,'uint32');
        this.recordSize = this.get_int_value(this.file_pointer,'uint32');
        this.stringBlockSize = this.get_int_value(this.file_pointer,'uint32');
        this.tableHash = this.get_int_value(this.file_pointer,'uint32');
        this.layoutHash = this.get_int_value(this.file_pointer,'uint32');
        this.minId = this.get_int_value(this.file_pointer,'uint32');
        this.maxId = this.get_int_value(this.file_pointer,'uint32');
        this.locale = this.get_int_value(this.file_pointer,'uint32');
        this.flags = this.get_int_value(this.file_pointer,'uint16');
        this.idField = this.get_int_value(this.file_pointer,'uint16');
        this.totalFieldCount = this.get_int_value(this.file_pointer,'uint32');
        this.bitpackedDataPos = this.get_int_value(this.file_pointer,'uint32');
        this.lookupColumnCount = this.get_int_value(this.file_pointer,'uint32');
        this.fieldStorageInfoSize = this.get_int_value(this.file_pointer,'uint32');
        this.commonBlockSize = this.get_int_value(this.file_pointer,'uint32');
        this.palletDataSize = this.get_int_value(this.file_pointer,'uint32');
        this.sectionCount = this.get_int_value(this.file_pointer,'uint32');

        console.log('Filepointer: ',this.file_pointer);






    }




    get_int_value(start,type = 'uint32') // default uint32
    {
        let lenght = 0; //uint32
        let part;

        if(type=='uint32')
        {
           lenght = 4; 
           this.file_pointer+=lenght; 
           part = this.file_buffer.slice(start,start+lenght);
           console.log(this.file_buffer.slice(start,start+lenght).readInt32LE());
           return this.file_buffer.slice(start,start+lenght).readInt32LE(); 
        }

        if(type=='uint16')
        {
           lenght = 2; 
           this.file_pointer+=lenght; 
           part = this.file_buffer.slice(start,start+lenght);
           console.log(this.file_buffer.slice(start,start+lenght).readInt16LE());
           return this.file_buffer.slice(start,start+lenght).readInt16LE(); 
        }


    }




    get_fileFormat()
    {
        let format = this.file_buffer.slice(0,4);
        this.fileFormat = format.toString();
    }

    get_file_buffer(callback)
    {
        fs.readFile(this.fileHandle, function (err, data) {
            if (err) throw err;
              // this.buffer = data;
               //this.buffer_data = new ArrayBuffer(data);
               callback(data);
          });   
    }


}

module.exports = Reader;