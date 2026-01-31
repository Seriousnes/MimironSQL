# Third-Party Notices

This file contains notices and acknowledgments for specifications, algorithms, and community resources used in this project.

## Acknowledgments

This project implements file format specifications and algorithms documented by various public sources:

### Salsa20 Stream Cipher

**Algorithm:** Salsa20  
**Author:** Daniel J. Bernstein  
**License:** Public Domain  
**Specification:** https://cr.yp.to/snuffle.html  

The Salsa20 implementation in this project follows the published specification. The algorithm and its constants are in the public domain.

### DB2 File Format Specification

**Format:** DB2/WDC5 (World of Warcraft Database Files)  
**Documentation:** https://wowdev.wiki/DB2  
**Community:** WoW Development Community  

This project implements the WDC5 format based on the publicly documented specification maintained by the WoW reverse engineering community. The implementation is original code written for this project.

### DBD File Format

**Format:** DBD (Database Definition files)  
**Repository:** https://github.com/wowdev/WoWDBDefs  
**Community:** WoW Development Community  

This project parses DBD files that define the structure of DB2 tables. The DBD file format and definitions are maintained by the WoW community. The parser implementation is original code written for this project.

## Related Open Source Projects

While this project is an independent implementation, we acknowledge these related projects in the ecosystem:

### DBCD

**Project:** DBCD  
**Repository:** https://github.com/wowdev/DBCD  
**License:** MIT License  
**Description:** A C# library for reading and writing DBC/DB2 files  

DBCD is another implementation of DB2 file parsing with a different architectural approach (dynamic, runtime-based). MimironSQL takes a different approach with a strongly-typed, compile-time LINQ query provider.

### WoWDBDefs

**Project:** WoWDBDefs  
**Repository:** https://github.com/wowdev/WoWDBDefs  
**Description:** Database definitions for World of Warcraft  

This project uses the definition files from WoWDBDefs to understand the structure of DB2 tables.

## Development Tools and Libraries

This project uses standard .NET libraries and NuGet packages, each with their own licenses:

- **.NET Runtime** - MIT License
- **System.Linq.Expressions** - MIT License
- **DBDefsLib** (if used) - MIT License
- **Other NuGet packages** - See individual package licenses

All NuGet dependencies are properly declared in project files (.csproj) and include their own license information.

---

## Note on Implementation

All code in this repository is original work, except where otherwise noted. Similarities to other implementations (particularly DBCD) are due to:

1. Both implementing the same publicly documented file formats
2. Both using similar domain terminology appropriate to the problem space
3. Both following standard C# coding conventions

The core architectures and implementation approaches are fundamentally different.

---

**Last Updated:** January 31, 2026
