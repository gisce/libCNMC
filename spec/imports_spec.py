from mamba import description, it


with description('Public resolution packages'):
    with it('imports the packages used by ERP modules'):
        import libcnmc.res_4131
        import libcnmc.res_4603
        import libcnmc.res_4666
        import libcnmc.res_4771
