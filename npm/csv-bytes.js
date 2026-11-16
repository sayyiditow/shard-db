'use strict'

// Input is one cell AFTER ordinary CSV delimiter/quote decoding.
function decodeCsvCellBytes(cell) {
  const source = typeof cell === 'string' ? Buffer.from(cell, 'utf8') : cell
  if (!(source instanceof Uint8Array)) throw new TypeError('cell must be a string or Uint8Array')
  function scan(output) {
    let count = 0
    for (let i = 0; i < source.length; i++) {
      let byte = source[i]
      if (byte === 92) {
        if (++i >= source.length) throw new SyntaxError('incomplete CSV byte escape')
        if (source[i] === 92) byte = 92
        else {
          if (source[i] !== 117 || i + 4 >= source.length || source[i + 1] !== 48 || source[i + 2] !== 48)
            throw new SyntaxError('invalid CSV byte escape')
          const hex = n => n >= 48 && n <= 57 ? n - 48 : n >= 65 && n <= 70 ? n - 55 : n >= 97 && n <= 102 ? n - 87 : -1
          const high = hex(source[i + 3]), low = hex(source[i + 4])
          if (high < 0 || low < 0) throw new SyntaxError('invalid CSV byte escape')
          byte = high * 16 + low; i += 4
        }
      }
      if (output) output[count] = byte
      count++
    }
    return count
  }
  const output = Buffer.alloc(scan(null)); scan(output); return output
}
module.exports = decodeCsvCellBytes
