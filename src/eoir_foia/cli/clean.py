"""CSV cleaning commands."""
import click
from pathlib import Path
import structlog
from typing import Optional

from eoir_foia.core.clean import (
    build_postfix,
    get_download_dir, 
    get_csv_files,
    clean_single_file,
    copy_csv_to_table,
    generate_validation_report
)
from eoir_foia.core.csv import CsvValidator

logger = structlog.get_logger()

@click.group()
def clean():
    """CSV cleaning operations."""
    pass


@clean.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.option(
    '--postfix', 
    help='Table postfix (e.g., 06_25). If not provided, uses latest download date.'
)
def choose(file_path: str, postfix: Optional[str]):
    """Clean a specific CSV file and write to database."""
    try:
        # Get postfix
        if not postfix:
            postfix = build_postfix()
            click.echo(f"Using postfix from latest download: {postfix}")
        
        csv_file = Path(file_path)
        click.echo(f"Processing: {csv_file.name}")
        
        # Setup progress bar
        file_size = csv_file.stat().st_size
        with click.progressbar(
            length=file_size,
            label=f'Cleaning {csv_file.name}',
            fill_char='=',
            empty_char='-'
        ) as bar:
            
            # Process the file
            result = clean_single_file(csv_file, postfix)
            bar.update(file_size)  # Complete the progress bar
        
        # Display results
        if result['success']:
            click.echo(click.style("✅ SUCCESS", fg='green'))
            click.echo(f"Table: {result['table_name']}")
            click.echo(f"Rows processed: {result['rows_processed']:,}")
            click.echo(f"Rows loaded: {result['rows_loaded']:,}")
            click.echo(f"File size: {result['file_size_mb']:.1f} MB")
            
            if result['total_modifications'] > 0:
                click.echo(click.style(f"⚠️  {result['total_modifications']} row modifications", fg='yellow'))
            
            if result['empty_primary_keys'] > 0:
                click.echo(click.style(f"⚠️  {result['empty_primary_keys']} empty primary keys", fg='yellow'))
                
            if result['data_quality_issues'] > 0:
                click.echo(click.style(f"⚠️  {result['data_quality_issues']} data quality issues", fg='yellow'))
        else:
            click.echo(click.style("❌ FAILED", fg='red'))
            click.echo(f"Error: {result['error']}")
            
    except Exception as e:
        logger.error("Clean choose failed", error=str(e))
        raise click.ClickException(str(e))


@clean.command()
@click.option(
    '--path',
    help='Directory containing CSV files. If not provided, uses latest download directory.'
)
@click.option(
    '--postfix',
    help='Table postfix (e.g., 06_25). If not provided, uses latest download date.'
)
def all(path: Optional[str], postfix: Optional[str]):
    """Clean all CSV files in directory and write to database."""
    try:
        # Get directory and postfix
        directory = get_download_dir(path)
        if not postfix:
            postfix = build_postfix()
            click.echo(f"Using postfix from latest download: {postfix}")
        
        click.echo(f"Processing directory: {directory}")
        
        # Get CSV files
        csv_files = get_csv_files(directory)
        if not csv_files:
            raise click.ClickException(f"No CSV files found in {directory}")
        
        click.echo(f"Found {len(csv_files)} CSV files")
        
        # Process files with progress tracking
        results = []
        total_rows_processed = 0
        total_rows_loaded = 0
        
        with click.progressbar(csv_files, label='Processing files') as files:
            for csv_file in files:
                result = clean_single_file(csv_file, postfix)
                results.append(result)
                total_rows_processed += result.get('rows_processed', 0)
                total_rows_loaded += result.get('rows_loaded', 0)
        
        # Summary report
        successful = len([r for r in results if r.get('success', False)])
        failed = len(results) - successful
        
        click.echo("\n" + "="*60)
        click.echo("BATCH PROCESSING SUMMARY")
        click.echo("="*60)
        click.echo(f"Files processed: {len(csv_files)}")
        click.echo(f"Successful: {successful}")
        if failed > 0:
            click.echo(click.style(f"Failed: {failed}", fg='red'))
        click.echo(f"Total rows processed: {total_rows_processed:,}")
        click.echo(f"Total rows loaded: {total_rows_loaded:,}")
        
        # Show individual file results
        click.echo(f"\nIndividual File Results:")
        for result in results:
            file_name = Path(result['csv_file']).name
            if result.get('success', False):
                status = click.style("✅", fg='green')
                details = f"{result['rows_loaded']:,} rows"
            else:
                status = click.style("❌", fg='red') 
                details = result.get('error', 'Unknown error')
            
            click.echo(f"  {status} {file_name:30s} {details}")
            
    except Exception as e:
        logger.error("Clean all failed", error=str(e))
        raise click.ClickException(str(e))


@clean.command()
@click.option(
    '--path',
    help='Directory containing Count.txt file. If not provided, uses latest download directory.'
)
@click.option(
    '--postfix',
    help='Table postfix (e.g., 06_25). If not provided, uses latest download date.'
)
def validate(path: Optional[str], postfix: Optional[str]):
    """Validate loaded data against source file counts."""
    try:
        # Get directory and postfix
        directory = get_download_dir(path)
        if not postfix:
            postfix = build_postfix()
            click.echo(f"Using postfix from latest download: {postfix}")
        
        click.echo(f"Validating data in directory: {directory}")
        click.echo(f"Using table postfix: {postfix}")
        
        # Generate validation report
        with click.progressbar(label='Comparing counts', length=100) as bar:
            report = generate_validation_report(directory, postfix)
            bar.update(100)
        
        # Display validation results
        click.echo("\n" + "="*60)
        click.echo("VALIDATION REPORT")
        click.echo("="*60)
        
        # Summary statistics
        click.echo(f"Files compared: {report['files_compared']}")
        click.echo(f"Perfect matches: {report['perfect_matches']}")
        click.echo(f"Total expected rows: {report['total_expected']:,}")
        click.echo(f"Total actual rows: {report['total_actual']:,}")
        
        total_diff = report['total_difference']
        if total_diff == 0:
            click.echo(click.style("✅ PERFECT MATCH", fg='green'))
        else:
            diff_pct = report['total_difference_pct']
            status_color = 'yellow' if abs(diff_pct) < 5 else 'red'
            click.echo(click.style(f"⚠️  Total difference: {total_diff:+,} ({diff_pct:+.2f}%)", fg=status_color))
        
        # Missing tables
        if report['missing_tables']:
            click.echo(click.style(f"\n❌ Missing tables: {len(report['missing_tables'])}", fg='red'))
            for table in report['missing_tables']:
                click.echo(f"  - {table}")
        
        # Detailed differences
        click.echo(f"\nDetailed Results:")
        click.echo(f"{'File':<30} {'Expected':<12} {'Actual':<12} {'Diff':<10} {'Status'}")
        click.echo("-" * 75)
        
        for csv_file, details in report['differences'].items():
            expected = details['expected']
            actual = details['actual']
            diff = details['difference']
            status = details['status']
            
            # Color code the status
            if status == 'match':
                status_display = click.style("✅ Match", fg='green')
            elif status == 'over':
                status_display = click.style(f"⬆️  +{diff:,}", fg='yellow')
            else:
                status_display = click.style(f"⬇️  {diff:,}", fg='red')
            
            click.echo(f"{csv_file:<30} {expected:<12,} {actual:<12,} {diff:<+10,} {status_display}")
            
    except Exception as e:
        logger.error("Clean validate failed", error=str(e))
        raise click.ClickException(str(e))
