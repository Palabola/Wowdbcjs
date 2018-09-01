const fs = require('fs');

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

class Reader {

    constructor(filepath)
    {
    
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

        this.hasEmbeddedStrings = (this.flags & 1) > 0;
        this.hasIdBlock = (this.flags & 1) > 0;

        let eof = 0;
        let hasRelationshipData = false;
        let recordCountSum = 0;

        this.sectionheaders = []; 

        for(let i=0;i < this.sectionCount;i++)
        {
            let section = {};
            console.log('Section header: ',i);
            section['unk1'] = this.get_int_value(this.file_pointer,'uint32');
            section['unk2'] = this.get_int_value(this.file_pointer,'uint32');
            section['offset'] = this.get_int_value(this.file_pointer,'uint32');
            section['recordCount'] = this.get_int_value(this.file_pointer,'uint32');
            section['stringBlockSize'] = this.get_int_value(this.file_pointer,'uint32');
            section['copyBlockSize'] = this.get_int_value(this.file_pointer,'uint32');
            section['indexBlockPos'] = this.get_int_value(this.file_pointer,'uint32');
            section['idBlockSize'] = this.get_int_value(this.file_pointer,'uint32');
            section['relationshipDataSize'] = this.get_int_value(this.file_pointer,'uint32'); 

            if (!this.hasEmbeddedStrings) {
                section['stringBlockPos'] = section['offset'] + (this.recordCount * this.recordSize);
            } else {
                section['stringBlockSize'] = 0;
                section['stringBlockPos'] = section['indexBlockPos'] + 6 * (this.maxId - this.minId + 1); // indexBlockPos is absolute position in file
            }

            section['idBlockPos'] = section['stringBlockPos'] + section['stringBlockSize'];
            section['copyBlockPos'] = section['idBlockPos'] + section['idBlockSize'];
            section['relationshipDataPos'] = section['copyBlockPos'] + section['copyBlockSize'];
            hasRelationshipData |= section['relationshipDataSize'] > 0;

            eof += section['size'] = section['relationshipDataPos'] + section['relationshipDataSize'] - section['offset'];
            recordCountSum += section['recordCount'];

            this.sectionheaders[i] = this.ksort(section);
        }

    
        console.log(this.sectionheaders[0]);
        console.log('Filepointer: ',this.file_pointer);

        this.headerSize = this.file_pointer + this.fieldCount*4;

        //Errors
        if(this.recordCount != recordCountSum)
        {
            throw "Non match record count!";  
        }

        if (this.recordCount == 0) {
            return;
        }

        if (this.fieldStorageInfoSize != this.totalFieldCount * 24) {
            throw "Unexpected file!"; 
        }

        if (this.hasEmbeddedStrings) {
            if (this.hasIdBlock) {
                throw "File has no ID or String value"
            }
        

        if (this.sectionCount != 1) {
            throw 'File has embedded strings and %d sections, expected 1, aborting';
        }

         this.indexBlockPos = this.sectionheaders[0]['indexBlockPos'];
        }


        this.fieldStorageInfoPos = this.headerSize;
        this.palletDataPos = this.fieldStorageInfoPos + this.fieldStorageInfoSize;

        this.commonBlockPos = this.palletDataPos + this.palletDataSize;

        eof += this.commonBlockPos + this.commonBlockSize;
        if (eof != this.fileSize) {
            throw "Unexpected size: $eof, actual size";
        }


        this.recordFormat = [];

        for (let fieldId = 0; fieldId < this.fieldCount; fieldId++) 
        {
            this.recordFormat[fieldId] = {};

            this.recordFormat[fieldId]['bitShift'] = this.get_int_value(this.file_pointer,'uint16');  //unpack('sbitShift/voffset', fread($this->fileHandle, 4));
            this.recordFormat[fieldId]['offset'] = this.get_int_value(this.file_pointer,'uint16');


            this.recordFormat[fieldId]['valueLength'] = Math.max(1, Math.ceil((32 - this.recordFormat[fieldId]['bitShift']) / 8));
            this.recordFormat[fieldId]['size'] = this.recordFormat[fieldId]['valueLength'];
            this.recordFormat[fieldId]['type'] = (this.recordFormat[fieldId]['size'] != 4) ? FIELD_TYPE_INT : FIELD_TYPE_UNKNOWN;
            if (this.hasEmbeddedStrings && this.recordFormat[fieldId]['type'] == FIELD_TYPE_UNKNOWN && !stringFields && fieldI.includes(stringFields)) 
            {
                this.recordFormat[fieldId]['type'] = FIELD_TYPE_STRING;
            }
            this.recordFormat[fieldId]['signed'] = false;
            if (fieldId > 0) {
                this.recordFormat[fieldId - 1]['valueCount'] =
                Math.floor((this.recordFormat[fieldId]['offset'] - this.recordFormat[fieldId - 1]['offset']) / this.recordFormat[fieldId - 1]['valueLength']);
            }
        }

        console.log('Filepointer: ',this.file_pointer);

        console.log(this.recordFormat);


        let fieldId = this.fieldCount - 1;
        let remainingBytes = this.recordSize - this.recordFormat[fieldId]['offset'];
        this.recordFormat[fieldId]['valueCount'] = Math.max(1, Math.floor(remainingBytes / this.recordFormat[fieldId]['valueLength']));
        if (this.recordFormat[fieldId]['valueCount'] > 1 &&    // we're guessing the last field is an array
            ((this.recordSize % 4 == 0 && remainingBytes <= 4) // records may be padded to word length and the last field size <= word size
             || (!this.hasIdBlock && this.idField == fieldId)// or the reported ID field is the last field
             || this.hasEmbeddedStrings)) {                     // or we have embedded strings
            this.recordFormat[fieldId]['valueCount'] = 1;      // assume the last field is scalar, and the remaining bytes are just padding
        }

        let commonBlockPointer = 0;
        let palletBlockPointer = 0;

        // Re-check file pointer!
        if(this.file_pointer != this.fieldStorageInfoPos){
                this.file_pointer  = this.fieldStorageInfoPos;                  
        }



       // $storageInfoFormat = 'voffsetBits/vsizeBits/VadditionalDataSize/VstorageType/VbitpackOffsetBits/VbitpackSizeBits/VarrayCount';

        for (let fieldId = 0; fieldId < this.fieldCount; fieldId++) {
            //parts = unpack($storageInfoFormat, fread($this->fileHandle, 24));

            let parts = {}

            parts['offsetBits'] = this.get_int_value(this.file_pointer,'uint16');
            parts['sizeBits'] = this.get_int_value(this.file_pointer,'uint16');
            parts['additionalDataSize'] = this.get_int_value(this.file_pointer,'uint32');
            parts['storageType'] = this.get_int_value(this.file_pointer,'uint32');
            parts['bitpackOffsetBits'] = this.get_int_value(this.file_pointer,'uint32');
            parts['bitpackSizeBits'] = this.get_int_value(this.file_pointer,'uint32');
            parts['arrayCount'] = this.get_int_value(this.file_pointer,'uint32');
  

            switch (parts['storageType']) {
                case FIELD_COMPRESSION_COMMON:
                  /*  this.recordFormat[fieldId]['size'] = 4;
                    this.recordFormat[fieldId]['type'] = FIELD_TYPE_INT;
                    this.recordFormat[fieldId]['valueCount'] = 1;
                    parts['defaultValue'] = pack('V', parts['bitpackOffsetBits']);
                    parts['bitpackOffsetBits'] = 0;
                    parts['blockOffset'] = $commonBlockPointer;
                    $commonBlockPointer += parts['additionalDataSize'];*/
                    break;
                case FIELD_COMPRESSION_BITPACKED_SIGNED:
                    this.recordFormat[fieldId]['signed'] = true;
                    // fall through
                case FIELD_COMPRESSION_BITPACKED:
                    this.recordFormat[fieldId]['size'] = 4;
                    this.recordFormat[fieldId]['type'] = FIELD_TYPE_INT;
                    this.recordFormat[fieldId]['offset'] = Math.floor(parts['offsetBits'] / 8);
                    this.recordFormat[fieldId]['valueLength'] =  Math.ceil((parts['offsetBits'] + parts['sizeBits']) / 8) - this.recordFormat[fieldId]['offset'] + 1;
                    this.recordFormat[fieldId]['valueCount'] = 1;
                    break;
                case FIELD_COMPRESSION_BITPACKED_INDEXED:
                case FIELD_COMPRESSION_BITPACKED_INDEXED_ARRAY:
                 /*   this.recordFormat[fieldId]['size'] = static::guessPalletFieldSize($palletBlockPointer, parts['additionalDataSize']);
                    this.recordFormat[fieldId]['type'] =
                        this.recordFormat[fieldId]['size'] == 4 ?
                            static::guessPalletFieldType($palletBlockPointer, parts['additionalDataSize']) :
                            FIELD_TYPE_INT;
                    this.recordFormat[fieldId]['offset'] = floor(parts['offsetBits'] / 8);
                    this.recordFormat[fieldId]['valueLength'] = ceil((parts['offsetBits'] + parts['sizeBits']) / 8) - this.recordFormat[fieldId]['offset'] + 1;
                    this.recordFormat[fieldId]['valueCount'] = parts['arrayCount'] > 0 ? parts['arrayCount'] : 1;
                    parts['blockOffset'] = $palletBlockPointer;
                    $palletBlockPointer += parts['additionalDataSize'];*/
                    break;
                case FIELD_COMPRESSION_NONE:
                    if (parts['arrayCount'] > 0) {
                        this.recordFormat[fieldId]['valueCount'] = parts['arrayCount'];
                    }
                    break;
                default:
                    throw "Unknown field compression type ID: %d";
            }

            this.recordFormat[fieldId]['storage'] = parts;
        }
        //End loop

        console.log('Record format:',this.recordFormat[0]['storage']);

        console.log(this.fileSize);
        console.log(this.file_pointer);



        console.log("Record buffer: ",this.file_buffer.slice(this.file_pointer,this.fileSize));

        console.log("Record count: ",this.recordCount);
        console.log("Record size: ",this.recordSize);
        console.log("Storage type: ",this.recordFormat[0]['storage']['storageType']);

        // Get record values
        for(let i = 0; i < this.recordCount;i++)
        {

           let value =  this.get_record_value(this.file_pointer,this.recordSize,this.recordFormat[0]['storage']['storageType']);

           console.log('ID: '+(i+1)+" value: "+value);


        }








        if (!this.hasIdBlock) {
            if (this.idField >= this.fieldCount) {
                "Expected ID field " + this.idField +  " does not exist. Only found " + this.fieldCount + " fields.";
            }
            if (this.recordFormat[this.idField]['valueCount'] != 1) {
                throw "Expected ID field " + this.idField + " reportedly has " + this.recordFormat[this.idField]['valueCount'] + " values per row";
            }
        }

        if (hasRelationshipData) {
            this.recordFormat[this.totalFieldCount++] = {
                'valueLength' : 4,
                'size' : 4,
                'offset' : this.recordSize,
                'type' : FIELD_TYPE_INT,
                'valueCount' : 1,
                'signed' : false,
                'storage' : {
                'storageType' : FIELD_COMPRESSION_NONE
            }
            };
        }


        if (this.hasEmbeddedStrings) {
            for (let fieldId = 0; fieldId < this.fieldCount; fieldId++) {
                if (this.recordFormat[fieldId]['storage']['storageType'] != FIELD_COMPRESSION_NONE) {
                    throw "DB2 with Embedded Strings has compressed field $fieldId";
                }
                delete  this.recordFormat[fieldId]['offset'];
            }

          //  $this->populateRecordOffsets();

            if (!stringFields) {
              //  this.detectEmbeddedStringFields();
            }
        }





    }


/*
    populateIdMap()
    {
        this.idMap = [];

        if (!this.hasIdBlock) {
            this.recordFormat[this.idField]['signed'] = false; // in case it's a 32-bit int

            let idOffset = !this.recordFormat[this.idField]['storage'] || this.recordFormat[this.idField]['storage']['storageType'] == FIELD_COMPRESSION_NONE;
            if (idOffset) {
                idOffset = this.recordFormat[this.idField]['offset'];
            }

            let sectionCount = this.sectionCount ? this.sectionCount : 1;

            let recIndex = 0;
            let recordCount = this.recordCount;

            for (let z = 0; z < sectionCount; z++) {
                if (this.sectionCount) {
                    recordCount = this.sectionHeaders[$z]['recordCount'];
                }
                if (idOffset !== false) {
                    // attempt shortcut so we don't have to parse the whole record

                    if (this.sectionCount) {
                        fseek($this->fileHandle, $this->sectionHeaders[$z]['offset'] + $idOffset);
                    } else {
                        fseek($this->fileHandle, $this->headerSize + $idOffset);
                    }

                    for ($x = 0; $x < $recordCount; $x++) {
                        $id = current(unpack('V', str_pad(fread($this->fileHandle, $this->recordFormat[$this->idField]['size']), 4, "\x00", STR_PAD_RIGHT)));
                        $this->idMap[$id] = $recIndex++;
                        fseek($this->fileHandle, $this->recordSize - $this->recordFormat[$this->idField]['size'], SEEK_CUR); // subtract for the bytes we just read
                    }
                } else {
                    for ($x = 0; $x < $recordCount; $x++) {
                        $rec = $this->getRecordByOffset($recIndex, false);
                        $id  = $rec[$this->idField];
                        $this->idMap[$id] = $recIndex++;
                    }
                }
            }
        } else {
            if ($this->sectionCount) {
                $recIndex = 0;
                for ($z = 0; $z < $this->sectionCount; $z++) {
                    fseek($this->fileHandle, $this->sectionHeaders[$z]['idBlockPos']);
                    for ($x = 0; $x < $this->recordCount; $x++) {
                        $this->idMap[current(unpack('V', fread($this->fileHandle, 4)))] = $recIndex++;
                    }
                }
            } else {
                fseek($this->fileHandle, $this->idBlockPos);
                if ($this->fileFormat == 'WDB2') {
                    for ($x = $this->minId; $x <= $this->maxId; $x++) {
                        $record = current(unpack('V', fread($this->fileHandle, 4)));
                        if ($record) {
                            $this->idMap[$x] = $record - 1;
                        }
                        fseek($this->fileHandle, 2, SEEK_CUR); // ignore embed string length in this record
                    }
                } else {
                    for ($x = 0; $x < $this->recordCount; $x++) {
                        $this->idMap[current(unpack('V', fread($this->fileHandle, 4)))] = $x;
                    }
                }
            }
        }

        $sections = $this->sectionHeaders;
        if (!$sections) {
            $sections = [[
                'copyBlockSize' => $this->copyBlockSize,
                'copyBlockPos' => $this->copyBlockPos,
            ]];
        }

        foreach ($sections as &$section) {
            if ($section['copyBlockSize']) {
                fseek($this->fileHandle, $section['copyBlockPos']);
                $entryCount = floor($section['copyBlockSize'] / 8);
                for ($x = 0; $x < $entryCount; $x++) {
                    list($newId, $existingId) = array_values(unpack('V*', fread($this->fileHandle, 8)));
                    if (!isset($this->idMap[$existingId])) {
                        throw new \Exception("Copy block referenced ID $existingId which does not exist");
                    }
                    $this->idMap[$newId] = $this->idMap[$existingId];
                }
                ksort($this->idMap, SORT_NUMERIC);
            }
        }
        unset($section);

















    }
*/


    get_record_value(start,size,storageType)
    {
       let lenght = size;
       let part;
       
       if(storageType==1) // uint32
       {
         lenght = 4;
         this.file_pointer+=lenght; 
         part = this.file_buffer.slice(start,start+size);   
         return part.readUIntLE(0,size); 
       }

       if(storageType==5) // int8-64
       {
         lenght = size;
         this.file_pointer+=lenght; 
         part = this.file_buffer.slice(start,start+size);   
         return part.readUIntLE(0,size);  
       }

       


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
           //console.log(this.file_buffer.slice(start,start+lenght).readInt32LE());
           return this.file_buffer.slice(start,start+lenght).readInt32LE(); 
        }

        if(type=='uint16')
        {
           lenght = 2; 
           this.file_pointer+=lenght; 
           part = this.file_buffer.slice(start,start+lenght);
          // console.log(this.file_buffer.slice(start,start+lenght).readInt16LE());
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


    // Helper things

    ksort(obj){
        var keys = Object.keys(obj).sort()
          , sortedObj = {};
      
        for(var i in keys) {
          sortedObj[keys[i]] = obj[keys[i]];
        }
      
        return sortedObj;
      }


}

module.exports = Reader;