/* This file is a simple serial implementation of the folloing algorithm:

for read in file:
    if median(read) > C:
        discard();
    else:
        count(read);
        save_to_file(read);
*/

#include <iostream>
#include <stdlib.h>
#include "parsers.hh"

using namespace std;

int main(int argc, char **argv)
{
    /* Check commandline arguments */
    if (argc != 2)
    {
        cout << "You must specify a filename" << endl;
        cout << "\tUsage: " << argv[0] << " filename" << endl;
        exit(-1);
    }

    /* For now, read in each read and just print it out */
    FastaParser p(argv[1]);

    Read r;

    while (!p.is_complete())
    {
        r = p.get_next_read();
        cout << r.name << endl;
        cout << r.seq << endl;
    }
}
