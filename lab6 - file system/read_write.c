#include "testfs.h"
#include "list.h"
#include "super.h"
#include "block.h"
#include "inode.h"

#define NR_DOUBLE_INDIR_BLOCKS (NR_INDIRECT_BLOCKS * NR_INDIRECT_BLOCKS)
/* given logical block number, read the corresponding physical block into block.
 * return physical block number.
 * returns 0 if physical block does not exist.
 * returns negative value on other errors. */
static int
testfs_read_block(struct inode *in, int log_block_nr, char *block)
{
	int phy_block_nr = 0;

	assert(log_block_nr >= 0);
	if (log_block_nr < NR_DIRECT_BLOCKS) {
		phy_block_nr = (int)in->in.i_block_nr[log_block_nr];
	} else {
		log_block_nr -= NR_DIRECT_BLOCKS;

		if (log_block_nr >= NR_INDIRECT_BLOCKS){
                    // should read from double linked indir blocks
                    if(in->in.i_dindirect > 0){
                        read_blocks(in->sb, block, in->in.i_dindirect, 1);
                        phy_block_nr = ((int *)block)[log_block_nr];
                    }
                }
		if (in->in.i_indirect > 0) {
			read_blocks(in->sb, block, in->in.i_indirect, 1);
			phy_block_nr = ((int *)block)[log_block_nr];
		}
	}
	if (phy_block_nr > 0) {
		read_blocks(in->sb, block, phy_block_nr, 1);
	} else {
		/* we support sparse files by zeroing out a block that is not
		 * allocated on disk. */
		bzero(block, BLOCK_SIZE);
	}
	return phy_block_nr;
}

int
testfs_read_data(struct inode *in, char *buf, off_t start, size_t size)
{
	char block[BLOCK_SIZE];
	long block_nr = start / BLOCK_SIZE;
	long block_ix = start % BLOCK_SIZE;
	int ret;

	assert(buf);
	if (start + (off_t) size > in->in.i_size) {
		size = in->in.i_size - start;
	}
	if (block_ix + size > BLOCK_SIZE) {
            // reads across multiple blocks
            long start_idx = block_ix;
            long byte_read = 0;
            long size_left = (long) size;
            long curr_block = block_nr;
            *buf = '\0';
            block[BLOCK_SIZE] = '\0';
            while(size_left>0){
                ret = testfs_read_block(in, curr_block, block);
                if (ret<0) break;
                byte_read = BLOCK_SIZE - start_idx;
                strcat(buf, block + start_idx);
                size_left -= byte_read;
                start_idx = 0;
                curr_block++;
            } 
            if(size_left <= 0) return size;
            return ret;
	}
	if ((ret = testfs_read_block(in, block_nr, block)) < 0)
		return ret;
	memcpy(buf, block + block_ix, size);
	/* return the number of bytes read or any error */
	return size;
}

/* given logical block number, allocate a new physical block, if it does not
 * exist already, and return the physical block number that is allocated.
 * returns negative value on error. */
static int
testfs_allocate_block(struct inode *in, int log_block_nr, char *block)
{
	int phy_block_nr;
	char indirect[BLOCK_SIZE];
        char double_indirect[NR_DOUBLE_INDIR_BLOCKS];
	int indirect_allocated = 0;

	assert(log_block_nr >= 0);
	phy_block_nr = testfs_read_block(in, log_block_nr, block);

	/* phy_block_nr > 0: block exists, so we don't need to allocate it, 
	   phy_block_nr < 0: some error */
	if (phy_block_nr != 0)
		return phy_block_nr;

	/* allocate a direct block */
	if (log_block_nr < NR_DIRECT_BLOCKS) {
		assert(in->in.i_block_nr[log_block_nr] == 0);
		phy_block_nr = testfs_alloc_block_for_inode(in);
		if (phy_block_nr >= 0) {
			in->in.i_block_nr[log_block_nr] = phy_block_nr;
		}
		return phy_block_nr;
	}

	log_block_nr -= NR_DIRECT_BLOCKS;
	if (log_block_nr >= NR_INDIRECT_BLOCKS){
            /* allocate a doubly indirect block */
            if (in->in.i_dindirect == 0) {	
		phy_block_nr = testfs_alloc_block_for_inode(in);
		if (phy_block_nr < 0)
			return phy_block_nr;
		bzero(double_indirect, NR_DOUBLE_INDIR_BLOCKS);
                indirect_allocated = 1;
		in->in.i_dindirect = phy_block_nr;
            } else {	/* read doubly indirect block */
		read_blocks(in->sb, double_indirect, in->in.i_dindirect, 1);
            }
            assert(((int *)double_indirect)[log_block_nr] == 0);	
            phy_block_nr = testfs_alloc_block_for_inode(in);

            if (phy_block_nr >= 0) {
                    /* update double indirect block */
                ((int *)double_indirect)[log_block_nr] = phy_block_nr;
                write_blocks(in->sb, double_indirect, in->in.i_dindirect, 1);

            } else if (indirect_allocated) {
                    /* free the indirect block that was allocated */
                    testfs_free_block_from_inode(in, in->in.i_dindirect);
                    in->in.i_dindirect = 0;
            }
            return phy_block_nr;
        }
            
	if (in->in.i_indirect == 0) {	/* allocate an indirect block */
		bzero(indirect, BLOCK_SIZE);
		phy_block_nr = testfs_alloc_block_for_inode(in);
		if (phy_block_nr < 0)
			return phy_block_nr;
		indirect_allocated = 1;
		in->in.i_indirect = phy_block_nr;
	} else {	/* read indirect block */
		read_blocks(in->sb, indirect, in->in.i_indirect, 1);
	}

	/* allocate direct block */
	assert(((int *)indirect)[log_block_nr] == 0);	
	phy_block_nr = testfs_alloc_block_for_inode(in);

	if (phy_block_nr >= 0) {
		/* update indirect block */
            ((int *)indirect)[log_block_nr] = phy_block_nr;
            write_blocks(in->sb, indirect, in->in.i_indirect, 1);
            
	} else if (indirect_allocated) {
		/* free the indirect block that was allocated */
		testfs_free_block_from_inode(in, in->in.i_indirect);
		in->in.i_indirect = 0;
	}
	return phy_block_nr;
}

int
testfs_write_data(struct inode *in, const char *buf, off_t start, size_t size)
{
	char block[BLOCK_SIZE];
	long block_nr = start / BLOCK_SIZE;
	long block_ix = start % BLOCK_SIZE;
	int ret;

	if (block_ix + size > BLOCK_SIZE) {
            long start_idx = block_ix;
            long byte_write = 0;
            long size_left = (long) size;
            long curr_block = block_nr;
            long offset = 0;
            while(size_left>0){
                ret = testfs_allocate_block(in, curr_block, block);
                if (ret<0) break;
                byte_write = BLOCK_SIZE - start_idx;
                memcpy(block + start_idx, buf + offset, byte_write);
                write_blocks(in->sb, block, ret, 1);
                size_left -= byte_write;
                offset += byte_write;
                start_idx = 0;
                curr_block++;
             } 
            if(size_left <= 0){
                in->in.i_size = MAX(in->in.i_size, start + (off_t) size);
                in->i_flags |= I_FLAGS_DIRTY;
                return size;
            }
            return ret;
	}
	/* ret is the newly allocated physical block number */
	ret = testfs_allocate_block(in, block_nr, block);
	if (ret < 0)
		return ret;
	memcpy(block + block_ix, buf, size);
	write_blocks(in->sb, block, ret, 1);
	/* increment i_size by the number of bytes written. */
	if (size > 0)
		in->in.i_size = MAX(in->in.i_size, start + (off_t) size);
	in->i_flags |= I_FLAGS_DIRTY;
	/* return the number of bytes written or any error */
	return size;
}

int
testfs_free_blocks(struct inode *in)
{
	int i;
	int e_block_nr;

	/* last block number */
	e_block_nr = DIVROUNDUP(in->in.i_size, BLOCK_SIZE);

	/* remove direct blocks */
	for (i = 0; i < e_block_nr && i < NR_DIRECT_BLOCKS; i++) {
		if (in->in.i_block_nr[i] == 0)
			continue;
		testfs_free_block_from_inode(in, in->in.i_block_nr[i]);
		in->in.i_block_nr[i] = 0;
	}
	e_block_nr -= NR_DIRECT_BLOCKS;

	/* remove indirect blocks */
	if (in->in.i_indirect > 0) {
		char block[BLOCK_SIZE];
		read_blocks(in->sb, block, in->in.i_indirect, 1);
		for (i = 0; i < e_block_nr && i < NR_INDIRECT_BLOCKS; i++) {
			testfs_free_block_from_inode(in, ((int *)block)[i]);
			((int *)block)[i] = 0;
		}
		testfs_free_block_from_inode(in, in->in.i_indirect);
		in->in.i_indirect = 0;
	}

	e_block_nr -= NR_INDIRECT_BLOCKS;
	if (e_block_nr >= 0) {
            if (in->in.i_dindirect > 0) {
		char block[NR_DOUBLE_INDIR_BLOCKS];
		read_blocks(in->sb, block, in->in.i_dindirect, 1);
                //int NR_DOUBLE_INDIR_BLOCKS = 4196362;
		for (i = 0; i < e_block_nr && i < NR_DOUBLE_INDIR_BLOCKS; i++) {
			testfs_free_block_from_inode(in, ((int *)block)[i]);
			((int *)block)[i] = 0;
		}
		testfs_free_block_from_inode(in, in->in.i_dindirect);
		in->in.i_dindirect = 0;
            }
	}

	in->in.i_size = 0;
	in->i_flags |= I_FLAGS_DIRTY;
	return 0;
}
